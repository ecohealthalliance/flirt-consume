from __future__ import print_function
import unittest
import os, sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import data
import boto3

class TestDataAccess(unittest.TestCase):

  """
  Confirm that eha-flirt S3 bucket is accessible
  """
  def test_s3_access(self):
    try:
      print("Test s3 access")
      s3 = boto3.resource("s3")
      flirt = s3.Bucket("eha-flirt")
      objs = list(flirt.objects.all())
      self.assertGreater(len(objs), 0)
    except Exception as e:
      print("**********Problem accessing s3.  Make sure the awscli has been setup on this machine.**********")
      raise e

  """
  Confirm we can access FTP
  """
  def test_ftp_access(self):
    try:
      print("Test FTP access")
      data.connect_to_ftp()
      files = data.get_ftp_entries()
      self.assertGreater(len(files),0)
    except Exception as e:
      print("**********Problem accessing FTP.  Make sure you have setup the settings file.**********")
      raise e