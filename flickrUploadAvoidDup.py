#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Upload to flickr and avoid duplicates.

Unittest: python -m unittest -v flickrUploadAvoidDup
Unittest: python -m unittest -v flickrUploadAvoidDup.TestSomeFlickrRoutines
"""
# https://github.com/sybrenstuvel/flickrapi
import flickrapi
import unittest
import argparse
import xml.etree.ElementTree as ET
import urllib
import os
import time
import sys
import logging
import hashlib
import sqlite3 as lite

take_saturartion_avoid_break = 60
local_db_filename = os.path.join(os.getenv('HOME'), 'flickruploadavoiddup.sqlite')
logfile =           os.path.join(os.getenv('HOME'), 'flickruploadavoiddup.log')

logging.basicConfig(
  format = '%(asctime)s %(name)s %(levelname)s: %(message)s',
  filename = logfile,
  level = logging.DEBUG)
#  level = logging.INFO)
mainlogger = logging.getLogger(__name__)
mainlogger.info('running flickr uploading client')

class FlickrAccess:
  def __init__(self):
    api_key =    unicode(os.environ['API_KEY'])
    api_secret = unicode(os.environ['API_SECRET'])
    self.logger = logging.getLogger(__name__ + '.FlickrAccess')
    self.flickr = flickrapi.FlickrAPI(api_key, api_secret, store_token = True)

  def ensurePermission(self, perm):
    uperm = unicode(perm)
    if not self.flickr.token_valid(perms=uperm):
      self.logger.info('acquire permission ' + uperm)
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

class LocalDB:
  def __init__(self):
    self.logger = logging.getLogger(__name__ + '.LocalDB')
    self.logger.info('opening SQLite file ' + local_db_filename)
    self.con = lite.connect(local_db_filename)
    self.cur = self.con.cursor()
    self.cur.execute("CREATE TABLE IF NOT EXISTS Uploaded (photoid INT, hash_o TEXT)")
    self.con.commit()

  def isregistered(self, filehash):
    self.cur.execute("SELECT * FROM Uploaded WHERE hash_o=?", (filehash,))
    res = self.cur.fetchall()
    if len(res) < 1:
      self.logger.debug('not registered in local DB. filehash: ' + filehash)
      return False
    else:
      self.logger.debug('is already registered in local DB (count: ' + str(len(res)) + '). filehash: ' + filehash)
      return True

  def register(self, photoId, filehash, checkForAlreadyInserted = False, immediateCommit = True):
    if checkForAlreadyInserted:
      # check, if hash has been already added
      ret = self.cur.execute("SELECT * FROM Uploaded WHERE hash_o=?", (filehash,))
      res = ret.fetchall()
      if len(res) > 0:
        self.logger.warn('photo with filehash ' + filehash + ' already registered (count: ' + str(len(res)) + '), put we are instructed to add another entry (new photoId: ' + photoId + ')')
    
    self.logger.debug('adding photoId: ' + photoId + ' with filehash ' + filehash)
    self.cur.execute("INSERT INTO Uploaded VALUES (?,?)", (photoId, filehash))

    if immediateCommit:
      self.con.commit()

  def commit(self):
    self.con.commit()

  def deregister(self, photoId = None, filehash = None):
    if photoId != None:
      self.logger.warn('removing photoId ' + photoId)
      self.cur.execute('DELETE FROM Uploaded WHERE photoId=?', (photoId,))
      self.logger.debug('removed ' + str(self.cur.rowcount) + ' rows')
      self.con.commit()

    if filehash != None:
      self.logger.warn('removing filehash ' + filehash)
      self.cur.execute('DELETE FROM Uploaded WHERE hash_o=?', (filehash,))
      self.logger.debug('removed ' + str(self.cur.rowcount) + ' rows')
      self.con.commit()

  def findDupOnHash(self):
    allDup = self.cur.execute('SELECT count(photoid), hash_o FROM uploaded group by hash_o having count(photoid)>1 order by count(photoid) desc').fetchall()
    for dup in allDup:
      fhash = dup[1]
      print 'hash_o = ' + fhash
      dupI = self.cur.execute('SELECT * FROM Uploaded WHERE hash_o=?', (fhash,)).fetchall()
      for dph in dupI:
        print('  ' + str(dph[0]))
    return None

class DuplicateAvoid:
  def __init__(self, flickraccess):
    assert flickraccess != None
    self.localdb = LocalDB()
    self.logger = logging.getLogger(__name__ + '.DuplicateAvoid')
    flickraccess.ensurePermission('write')
    self.flickr = flickraccess.flickr

  def hashoffile(self, filename):
    return hashlib.sha256(open(filename, 'rb').read()).hexdigest()

  def isalreadyuploaded_flickr(self, filehash):
    #####  https://github.com/sybrenstuvel/flickrapi/issues/88
    result = self.flickr.photos.search(userid = 'me', extras = 'machine_tags', machine_tags = unicode('hash:o="'+filehash+'"'))
    #ET.dump(result)
    ##for photo in self.flickr.walk(userid='me', machine_tags = unicode('hash:o="'+filehash+'"')):
    ##  ET.dump(photo)
    total = int(result.find('./photos').get('total'))
    return (total > 0, result)
    
  def suffix(self, filename, alreadyuploaded = False, uploaded = False):
    assert alreadyuploaded != uploaded
    if alreadyuploaded:
      suffix = '-already-uploaded.jpg'
    if uploaded:
      suffix = '-uploaded.jpg'

    try:
      os.rename(filename, filename + suffix)
    except:
      self.logger.exception('while renaming ' + filename)
      raise

  def getIdFromResult(self, result):
    ET.dump(result)
    raise 'xxx'

  def isalreadyuploaded(self, fname, filehash):
    if self.localdb.isregistered(filehash):
      return True
    # not found in local DB -> check in flickr with machinetag
    self.logger.debug('hash ' + filehash + ' not found in DB, now checking with machinetag in flickr')
    (alreadyUploaded, result) = self.isalreadyuploaded_flickr(filehash)
    if alreadyUploaded:
      self.logger.info('hash ' + filehash + ' found in flickr, but not DB -> adding hash to DB')
      self.localdb.register(self.getIdFromResult(result))
      return True
    else:
      self.logger.debug('hash ' + filehash + ' not found in flickr as machinetag')
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
    self.logger.info('starting walking through all photos on flickr in order to find photos without hash')
    walkingcount = 0
    for walkingphoto in self.flickr.walk(user_id = 'me', extras = 'machine_tags, url_o', per_page = '500'):
      walkingcount += 1
      photoid = walkingphoto.get('id')
      mtags =   walkingphoto.get('machine_tags')
      urlo =    walkingphoto.get('url_o')
      print('checking #' + str(walkingcount) + ': ' + photoid)
      if not self.hashashintags(mtags):
        self.updatemachinetags(photoid, urlo)

  def updatemachinetags(self, photo, urlo):
    photoid = str(photo)
    print(' updating photoid ' + photoid + ' on flickr with hash')
    self.logger.info('updating photoid ' + photoid + ' on flickr with hash')

    self.logger.debug('downloading photo (original) of ' + photoid)
    downloaded = urllib.urlretrieve(urlo)
    tmpfile =    downloaded[0]
    filehash =   self.hashoffile(tmpfile)
    self.logger.debug('downloaded ' + photoid + ' to ' + tmpfile + ' does have hash: ' + filehash)

    # we know it hasn't been set, as otherwise the flow hadn't reach this point
    self.logger.debug('setting machine tags for photoID ' + photoid)
    try:
      self.flickr.photos.addTags(photo_id=photoid, tags = 'hash:o=' + filehash)
    except:
      self.logger.exception('while tagging ' + photoid)
      raise

    # register and remove temporary file
    self.localdb.register(photoid, filehash)
    try:
      self.logger.debug('removing temporary file ' + tmpfile)
      os.remove(tmpfile)
    except:
      self.logger.exception('Could not remove ' + tmpfile)
      raise

  @staticmethod
  def hashashintags(tags):
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

  def updatedbfrommachinetags(self):
    "identify all photos with machinetag and update local sqlite DB"
    counter = 0
    for walkingphoto in self.flickr.walk(user_id = 'me', extras = 'machine_tags', per_page = '500'):
      counter += 1
      if counter % 100 == 0:
        print counter
        self.localdb.commit()
      photoid = walkingphoto.get('id')
      mtags =   walkingphoto.get('machine_tags')
      if len(mtags) != 0:
        filehash = mtags[len('hash:o='):]
        assert len(filehash) == len('b1d11fc4e4d551a502bd2fc9572b1e066b3a33a28e5c28e9ce59823ccaf6b83b')
        self.localdb.register(photoid, filehash, immediateCommit = False)
    self.localdb.commit()


class UploadFindDuplicate:
  def __init__(self, flickraccess):
    assert flickraccess != None
    flickraccess.ensurePermission('write')
    self.flickr = flickraccess.flickr
    self.avoider = DuplicateAvoid(flickraccess)
    self.localdb = self.avoider.localdb
    self.logger = logging.getLogger(__name__ + '.UploadFindDuplicate')

  def uploadfolder(self, folder = '.'):
    uploadCounter = 0
    for root, dirs, files in os.walk(folder):
      for fs in [f for f in files if f.lower().endswith('.jpg') and not f.lower().endswith('uploaded.jpg')]:
        uploadCounter += 1
        if take_saturartion_avoid_break > 0 and uploadCounter % 10 == 0:
          print("++ take a break, so we don't over-saturate our API key on flickr ++")
          self.logger.debug("break start: so we don't saturate our API key on flickr")
          time.sleep(take_saturartion_avoid_break)
          self.logger.debug("break over")

        fname = os.path.join(root, fs)
        if not self.uploadfile(fname):
          uploadCounter -= 1

    self.logger.info('all images processed')

  def uploadfile(self, fname):
    self.logger.debug('calculating SHA256 of ' + fname)
    filehash = self.avoider.hashoffile(fname)

    # check if file with this sha256 has already been uploaded
    if self.avoider.isalreadyuploaded(fname, filehash):
      self.logger.info('skipping upload as already uploaded: ' + fname + ', SHA256: ' + filehash)
      self.avoider.suffix(fname, alreadyuploaded = True)
      print('   skipping upload as already uploaded: ' + fname + ', SHA256: ' + filehash)
      return False

    fname = utf8safepath(fname)

    # uploading image
    try:
      self.logger.debug('uploading ' + fname)
      print('uploading ' + fname)
      try:
        up = self.flickr.upload(filename=fname, is_public=0, is_family=1, is_friend=0, tags = 'hash:o=' + filehash)
      except UnicodeDecodeError:
        xXXXX
      photoid = up.find('./photoid').text
      self.logger.info('uploaded ' + fname + ' as PhotoID ' + photoid + ' with hash: ' + filehash)
    except:
      self.logger.exception('unexpected exception while uploading ' + fname)
      raise

    # register locally
    self.localdb.register(photoid, filehash)
    self.avoider.suffix(fname, uploaded = True)
    return True

  def utf8safepath(self, path):
    try:
      path.encode('utf-8')
    except UnicodeDecodeError:
      newpath = safetxt(path)
      self.warn('UTF-8 issue with file ' + path + '. Renaming to: ' + newpath)
      try:
        os.rename(path, newpath)
      except:
        self.logger.exception('while renameing ' + path + ' to ' + newpath)
        raise
      path = newpath
    return path
  
  @staticmethod
  def safetxt(x):
    x = x.replace('ä', 'ae')
    x = x.replace('ö', 'oe')
    x = x.replace('ü', 'ue')
    x = x.replace('Ä', 'Ae')
    x = x.replace('Ö', 'Oe')
    x = x.replace('Ü', 'Ue')
    return x  

class FindDuplicate:
  def __init__(self):
    self.localdb = LocalDB()
    self.logger = logging.getLogger(__name__ + '.FindDuplicate')

  def findDuplicate(self):
    for line in self.localdb.findDupOnHash():
      print(line)
##
## Unit test
##

class TestSomeDetails(unittest.TestCase):
  def test_upper(self):
    self.assertEqual('foo'.upper(), 'FOO')

class TestSomeFlickrRoutines(unittest.TestCase):
  def test_hashShouldExist(self):
    # https://www.flickr.com/search/?text=9f38318b6ad55089f68cc2efc16e945a1fea21ba548f7f672416b65c227e675c --> 2 photos
    # https://www.flickr.com/photos/tags/hash:o=9f38318b6ad55089f68cc2efc16e945a1fea21ba548f7f672416b65c227e675c --> 1 photo
    (already, result) = DuplicateAvoid(FlickrAccess()).isalreadyuploaded_flickr('9f38318b6ad55089f68cc2efc16e945a1fea21ba548f7f672416b65c227e675c')
    #(already, result) = DuplicateAvoid(FlickrAccess()).isalreadyuploaded_flickr('9f383')
    logging.getLogger('TestSomeFlickrRoutines').debug('unittest search result: ' + ET.tostring(result))
    self.assertTrue(already)

  ##def test_hashShouldNotExist(self):
  ##  (already, result) = DuplicateAvoid(FlickrAccess()).isalreadyuploaded_flickr('9f38318b6ad55089f68cc2efc16e945a1fea21ba548f7f672416b65c227a111c')
  ##  self.assertFalse(already)

class TestSomeLocalDBRoutines(unittest.TestCase):
  def test_addCheckRemove(self):
    db = LocalDB()
    fakeFileHash = 'xxxxxx8b6ad55089f68cc2efc1xxxxxxxxxxx1ba548f7f672416b65c227exxxx'
    fakePhotoId = '99887766'
    db.register(fakePhotoId, fakeFileHash)
    self.assertTrue(db.isregistered(fakeFileHash))
    db.deregister(fakePhotoId, fakeFileHash)

class TestDuplicateAvoid(unittest.TestCase):
  def test_hash(self):
    self.assertFalse(DuplicateAvoid.hashashintags('sadkjsd'))
    self.assertFalse(DuplicateAvoid.hashashintags(None))
    self.assertTrue(DuplicateAvoid.hashashintags('hash:o=1387162378'))

##
## main
##

def main(argv):
  parser = argparse.ArgumentParser(description='Upload photos to flickr and avoid duplicates.')
  parser.add_argument('--upload', help='Upload and update local db, so the same file (based on hash) cannot be uploaded twice')
  parser.add_argument('--updateonflickr', action='store_true', help='Update all machinetags on flickr and local DB that currently doesn\'t yield an hash')
  parser.add_argument('--updatefromflickr', action='store_true', help='Update local DB based on hashes on flickr')
  parser.add_argument('--unittest', action='store_true', help='Run all unittests')
  parser.add_argument('--debug', action='store_true', help='Increase logging to debug')
  parser.add_argument('--finddup', action='store_true', help='Find duplicates based on local DB and provid suggestions for removal')
  args = parser.parse_args()
  print args

  if args.debug:
    logging.getLogger().setLevel(logging.DEBUG)

  mainlogger.debug(str(args))

  if args.upload:
    UploadFindDuplicate(FlickrAccess()).uploadfolder(args.upload)
  elif args.updateonflickr:
    DuplicateAvoid(FlickrAccess()).setemptymachinetags()
  elif args.updatefromflickr:
    DuplicateAvoid(FlickrAccess()).updatedbfrommachinetags()
  elif args.unittest:
    unittest.main()
  elif args.finddup:
    FindDuplicate().findDuplicate()
  else:
    print('no action indicated')
    sys.exit(1)

if __name__ == "__main__":
  main(sys.argv)
