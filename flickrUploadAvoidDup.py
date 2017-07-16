#!/usr/bin/env python
"""
Upload to flickr and avoid duplicates.

Unittest: python -m unittest -v uploadFindDuplicate
"""
import flickrapi
import unittest
import xml.etree.ElementTree as ET
import urllib
import os
import time
import sys
import getopt
import logging
import hashlib
import sqlite3 as lite

take_saturartion_avoid_break = 60
local_db_filename = os.path.join(os.getenv('HOME'), 'flickruploadavoiddup.sqlite')
api_key =    unicode(os.environ['API_KEY'])
api_secret = unicode(os.environ['API_SECRET'])

logging.basicConfig(
  format = '%(asctime)s %(name)s %(levelname)s: %(message)s',
  filename = 'flickr-uploader.log',
  level = logging.DEBUG)
#  level = logger.INFO)
logger = logging.getLogger(__name__)
logger.info('running flickr uploading client')

class FlickrAccess:
  def __init__(self):
    self.flickr = flickrapi.FlickrAPI(api_key, api_secret, store_token = True)

  def ensurePermission(self, perm):
    uperm = unicode(perm)
    if not self.flickr.token_valid(perms=uperm):
      logger.info('acquire permission ' + uperm)
      # Get a request token
      self.flickr.get_request_token(oauth_callback='oob')

      # Open a browser at the authentication URL. Do this however
      # you want, as long as the user visits that URL.
      authorize_url = self.flickr.auth_url(perms=uperm)
      #webbrowser.open_new_tab(authorize_url)
      print(' Please paste this URL into your browser and copy the verification code: ' + authorize_url)

      # Get the verifier code from the user. Do this however you
      # want, as long as the user gives the application the code.
      verifier = unicode(raw_input(' Verifier code: '))

      # Trade the request token for an access token
      self.flickr.get_access_token(verifier)

class DuplicateAvoid:
  def __init__(self, flickraccess):
    assert flickraccess != None

    logger.info('opening SQLite file ' + local_db_filename)
    self.con = lite.connect(local_db_filename)
    self.cur = self.con.cursor()
    self.cur.execute("CREATE TABLE IF NOT EXISTS Uploaded (photoid INT, hash_o TEXT)")
    self.con.commit()

    flickraccess.ensurePermission('write')
    self.flickr = flickraccess.flickr

  def hashoffile(self, filename):
    return hashlib.sha256(open(filename, 'rb').read()).hexdigest()

  def isalreadyuploaded(self, fname, filehash):
    # http://zetcode.com/db/sqlitepythontutorial/
    self.cur.execute("SELECT * FROM Uploaded WHERE hash_o=?", (filehash,))
    res = self.cur.fetchone()
    if not res == None:
      logger.info('file ' + fname + ' is already updated as by filehash: ' + filehash)
      self._suffixfile(fname, '-already-uploaded.jpg')
      return True

    # not found in local DB -> check in flickr with machinetag
    logger.debug('hash ' + filehash + ' not found in DB, now checking with machinetag in flickr')
    result = self.flickr.photos.search(userid = 'me', machine_tags = filehash)
    #ET.dump(result)
    total = int(result.find('./photos').get('total'))

    if total > 0:
      logger.info('hash ' + filehash + ' found (count: ' + str(total) + ')in flickr, but not DB -> adding hash to DB')
      try:
        # fetching id of FIRST photo, skip potential other photos
        photoid = result.find('./photos/photo[1]').get('id')
      except:
        logger.debug('exception while fetching ID of photo[1], now dumping object')
        ET.dump(result)
        logger.debug(ET.tostring(result))
        logger.exception('while parsing result')
        raise
      self._adduploadedhash(fname, photoid, filehash)
      self._suffixfile(fname, '-already-uploaded.jpg')
      return True
    else:
      logger.debug('hash ' + filehash + ' not found in flickr as machinetag')

    return False

  def _suffixfile(self, filename, suffix):
    try:
      os.rename(filename, filename + suffix)
    except:
      logger.exception('while renaming ' + filename)
      raise

  def _adduploadedhash(self, filename, photoid, filehash):
    # check, if hash has been already added
    ret = self.cur.execute("SELECT hash_o FROM Uploaded WHERE hash_o=?", (filehash,))
    if ret.fetchone() == None:
      # not found in local DB -> add to local DB
      self.cur.execute("INSERT INTO Uploaded VALUES (?,?)", (photoid, filehash))
      self.con.commit()
      return True
    else:
      # already in local DB -> nothing to do
      return False

  def listmultipleuploadedhashs(self):
    # select hash_o, count(hash_o) from uploaded group by hash_o having count(hash_o)>1
    ret = self.cur.execute('SELECT hash_o, COUNT(hash_o) FROM Uploaded GROUP BY hash_o HAVING COUNT(hash_o)>1')
    print(ret.fetchall())

  def setemptymachinetags(self):
    """walk through all photos in flickr, that doens't have a machinetag and update this value and also add to our local database"""
    #o = self.flickr.photos.getUntagged(user_id='me', extras='machine_tags, url_o')
    #ET.dump(o)

    # using flickr.photos.getUntagged(extras='machine_tags') is not reliable, as also non-machine_tags would be yield
    # as tagged and therefore wouldn't be listed.

    # using normal all image flickr.walk() (basically flickr.photos.search()) doesn't provide filters for non-existing machine tags
    logger.info('starting walking through all photos on flickr in order to find photos without hash')
    walkingcount = 0
    for walkingphoto in self.flickr.walk(user_id = 'me', extras = 'machine_tags, url_o', per_page = '500'):
      #ET.dump(walkingphoto)
      walkingcount += 1
      photoid = walkingphoto.get('id')
      mtags =   walkingphoto.get('machine_tags')
      urlo =    walkingphoto.get('url_o')
      print('checking #' + str(walkingcount) + ': ' + photoid)
      if not self.hashashintags(mtags):
        #self._updatemachinetags(resp.find('./photo'))
        self._updatemachinetags(photoid, urlo)

  def _updatemachinetags(self, photo, urlo = None):
    if type(photo).__name__ == '_Element':
      photoid = str(photo.get('id'))
    else:
      photoid = str(photo)
    print(' updating photoid ' + photoid + ' on flickr with hash')
    logger.info('updating photoid ' + photoid + ' on flickr with hash')
    if urlo == None:
      # assume photo is elementtree
      urlo = self.url(photo, 'o')

    logger.info('downloading photo (original) of ' + photoid)
    downloaded = urllib.urlretrieve(urlo)
    tmpfile =    downloaded[0]
    filehash =   self.hashoffile(tmpfile)
    logger.debug('downloaded ' + photoid + ' to ' + tmpfile + ' does have hash: ' + filehash)

    logger.debug('setting machine tags for photoID ' + photoid)
    try:
      self.flickr.photos.addTags(photo_id=photoid, tags = 'hash:o=' + filehash)
    except:
      logger.exception('while tagging ' + photoid)
      raise
    self._adduploadedhash('', photoid, filehash)
    try:
      logger.debug('removing temporary file ' + tmpfile)
      os.remove(tmpfile)
    except:
      logger.exception('Could not remove ' + tmpfile)
      raise

  def hashashintags(self, tags):
    if type(tags)==type('str'):
      # TODO need to check, how multi machine tags are handled
      return tags.startswith('hash:o=')

    if tags == None or len(tags)==0:
      return False
    for tag in tags:
      raw = tag.get('raw')
      if raw.startswith('hash:o='):
        return True
    return False

  def url(self, photo, kind):
    if type(photo)==type('str'):
      resp = self.flickr.photos.getInfo(photo_id = photo)
      photo = resp.find('./photo')
    # https://www.flickr.com/services/api/misc.urls.html
    farmid    = str(photo.get('farm'))
    serverid  = str(photo.get('server'))
    photoid   = str(photo.get('id'))
    secret    = str(photo.get('secret'))
    osecret   = str(photo.get('originalsecret'))

    if kind == 'o':
      return 'https://farm{}.staticflickr.com/{}/{}_{}_o.jpg'.format(farmid, serverid, photoid, osecret)
    else:
      return 'https://farm{}.staticflickr.com/{}/{}_{}_{}.jpg'.format(farmid, serverid, photoid, secret, kind)

  def updatedbfrommachinetags(self):
    "identify all photos with machinetag and update local sqlite DB"
    counter = 0
    for walkingphoto in self.flickr.walk(user_id = 'me', extras = 'machine_tags', per_page = '500'):
      counter += 1
      if counter % 100 == 0:
        print counter
      photoid = walkingphoto.get('id')
      mtags =   walkingphoto.get('machine_tags')
      if len(mtags) != 0:
        filehash = mtags[len('hash:o='):]
        assert len(filehash) == len('b1d11fc4e4d551a502bd2fc9572b1e066b3a33a28e5c28e9ce59823ccaf6b83b')
        self._adduploadedhash('', photoid, mtags)


class UploadFindDuplicate:
  def __init__(self, flickraccess):
    assert flickraccess != None
    flickraccess.ensurePermission('write')
    self.flickr = flickraccess.flickr
    self.avoider = DuplicateAvoid(flickraccess)

  def uploadfolder(self, folder = '.'):
    uploadCounter = 0
    for root, dirs, files in os.walk(folder):
      for fs in [f for f in files if f.lower().endswith('.jpg') and not f.lower().endswith('uploaded.jpg')]:
        uploadCounter += 1
        if take_saturartion_avoid_break > 0 and uploadCounter % 10 == 0:
          print("++ take a break, so we don't over-saturate our API key on flickr ++")
          logger.debug("break start: so we don't saturate our API key on flickr")
          time.sleep(take_saturartion_avoid_break)
          logger.debug("break over")

        fname = os.path.join(root, fs)
        if not self.uploadfile(fname):
          uploadCounter -= 1

    logger.info('all images processed')

  def uploadfile(self, fname):
    logger.debug('calculating SHA256 of ' + fname)
    filehash = self.avoider.hashoffile(fname)

    # check if file with this sha256 has already been uploaded
    if self.avoider.isalreadyuploaded(fname, filehash):
      logger.info('skipping upload as already uploaded: ' + fname + ', SHA256: ' + filehash)
      print('   skipping upload as already uploaded: ' + fname + ', SHA256: ' + filehash)
      return False

    # uploading image
    try:
      logger.debug('uploading ' + fname)
      print('uploading ' + fname)
      up = self.flickr.upload(filename=fname, is_public=0, is_family=1, is_friend=0, tags = 'hash:o=' + filehash)
      photoid = up.find('./photoid').text
      logger.info('uploaded ' + fname + ' as PhotoID ' + photoid + ' with hash: ' + filehash)
    except:
      logger.exception('unexpected exception while uploading ' + fname)
      raise

    # add filehash to sqlite DB
    # TODO call _ from out of class
    self.avoider._adduploadedhash(fname, photoid, filehash)

    try:
      os.rename(fname, fname + '-uploaded.jpg')
    except:
      logger.exception('could not rename ' + fname)
      raise
    return True


class TestSomeDetails(unittest.TestCase):
  def test_upper(self):
    self.assertEqual('foo'.upper(), 'FOO')

def main(argv):
  if len(argv) == 0:
    usage()
    sys.exit(2)
  try:
    opts, args = getopt.getopt(argv, "uof", ["upload", "updateonflickr", "updatefromflickr"])
  except getopt.GetoptError:
    usage()
    sys.exit(2)
  for opt, args in opts:
    if opt in ("-u", "--upload"):
      UploadFindDuplicate(FlickrAccess()).uploadfolder('.')
    elif opt in ("-o", "--updateonflickr"):
      DuplicateAvoid(FlickrAccess()).setemptymachinetags()
    elif opt in ("-f", "--updatefromflickr"):
      DuplicateAvoid(FlickrAccess()).updatedbfrommachinetags()
    else:
      print("unknown " + opt)

def usage():
  print("""Upload photos to flickr and avoid duplicates
-u, --upload            upload and update local db, so the same file (based on hash) cannot be uploaded twice
-o, --updateonflickr    update all machinetags on flickr and local DB that currently doesn't yield an hash
-f, --updatefromflickr  x
""")

if __name__ == "__main__":
  main(sys.argv[1:])
