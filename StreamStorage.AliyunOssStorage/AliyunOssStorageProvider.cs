using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace StreamStorage.AliyunOssStorage
{
    /// <summary>
    /// Aliyun Oss Storage Provider
    /// </summary>
    public class AliyunOssStorageProvider : IStreamStorageProvider
    {
        private string endpoint = "";
        private string accessKeyId = "";
        private string accessKeySecret = "";
        private string bucketName = "";
        private int optCountQuotaPerDay = 10000;// Operate count per day

        public string ProviderName { get { return "aliyun.oss"; } }

        public void Configure(Dictionary<string, string> config)
        {
            this.endpoint = config.ContainsKey("endpoint") ? config["endpoint"] : "";
            this.accessKeyId = config.ContainsKey("accessKeyId") ? config["accessKeyId"] : "";
            this.accessKeySecret = config.ContainsKey("accessKeySecret") ? config["accessKeySecret"] : "";
            this.bucketName = config.ContainsKey("bucketName") ? config["bucketName"] : "";
            string strOptCountQuotaPerDay = config.ContainsKey("optCountQuotaPerDay") ? config["optCountQuotaPerDay"] : "";
            if(!Int32.TryParse(strOptCountQuotaPerDay, out optCountQuotaPerDay))
            {
                optCountQuotaPerDay = 10000;
            }
        }
        
        public Stream GetObject(string objectName)
        {
            if (String.IsNullOrEmpty(objectName))
            {
                throw new ArgumentNullException("objectName");
            }
            try
            {
                var client = new Aliyun.OSS.OssClient(this.endpoint, this.accessKeyId, this.accessKeySecret);
                if (client.DoesObjectExist(this.bucketName, objectName))
                {
                    var obj = client.GetObject(this.bucketName, objectName);
                    return obj.Content;
                }
                else
                {
                    throw new StorageObjectNotFoundException("Storage object not found！", objectName);
                }
            }
            catch (StorageObjectNotFoundException notFound)
            {
                throw notFound;
            }
            catch (Exception ex)
            {
                throw new StorageIOException("Get object fail!", ex);
            }
        }
        
        public void PutObject(string objectName, Stream content, bool overrideIfExists)
        {
            if (String.IsNullOrEmpty(objectName))
            {
                throw new ArgumentNullException("objectName");
            }
            if (content == null || content == Stream.Null)
            {
                throw new ArgumentNullException("content");
            }
            try
            {
                var client = new Aliyun.OSS.OssClient(this.endpoint, this.accessKeyId, this.accessKeySecret);
                if (!client.DoesBucketExist(this.bucketName))
                {
                    client.CreateBucket(this.bucketName);
                }
                if (overrideIfExists || !client.DoesObjectExist(this.bucketName, objectName))
                {
                    client.PutObject(this.bucketName, objectName, content);
                }
            }
            catch (Exception ex)
            {
                throw new StorageIOException("Put object fail!", ex);
            }
        }
        
        public void DeleteObject(string objectName)
        {
            if (String.IsNullOrEmpty(objectName))
            {
                throw new ArgumentNullException("objectName");
            }
            try
            {
                var client = new Aliyun.OSS.OssClient(this.endpoint, this.accessKeyId, this.accessKeySecret);
                if (client.DoesObjectExist(this.bucketName, objectName))
                {
                    client.DeleteObject(this.bucketName, objectName);
                }
            }
            catch (Exception ex)
            {
                throw new StorageIOException("Delete object fail!", ex);
            }
        }
        
        public bool ObjectExists(string objectName)
        {
            if (String.IsNullOrEmpty(objectName))
            {
                throw new ArgumentNullException("objectName");
            }
            try
            {
                var client = new Aliyun.OSS.OssClient(this.endpoint, this.accessKeyId, this.accessKeySecret);
                return client.DoesObjectExist(this.bucketName, objectName);
            }
            catch (Exception ex)
            {
                throw new StorageIOException("Check object exists or not fail!", ex);
            }
        }
    }
}
