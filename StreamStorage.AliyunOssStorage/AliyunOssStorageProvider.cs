using System;
using System.Collections.Generic;
using System.IO;

namespace StreamStorage.AliyunOssStorage
{
    /// <summary>
    /// Aliyun Oss Storage Provider
    /// </summary>
    public class AliyunOssStorageProvider : IStreamStorageProvider
    {
        private Aliyun.OSS.OssClient client = null;
        private string endpoint = "";
        private string accessKeyId = "";
        private string accessKeySecret = "";
        private string bucketName = "";
        private int optCountQuotaPerDay = 10000;// Operate count per day
        private string objectMetadata_CacheControl = "";

        /// <summary>
        /// 
        /// </summary>
        public string ProviderName { get { return "aliyun.oss"; } }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="config"></param>
        public void Configure(Dictionary<string, string> config)
        {
            this.endpoint = config.ContainsKey("endpoint") ? config["endpoint"] : "";
            this.accessKeyId = config.ContainsKey("accessKeyId") ? config["accessKeyId"] : "";
            this.accessKeySecret = config.ContainsKey("accessKeySecret") ? config["accessKeySecret"] : "";
            client = new Aliyun.OSS.OssClient(this.endpoint, this.accessKeyId, this.accessKeySecret);

            this.bucketName = config.ContainsKey("bucketName") ? config["bucketName"] : "";
            string strOptCountQuotaPerDay = config.ContainsKey("optCountQuotaPerDay") ? config["optCountQuotaPerDay"] : "";
            if (!Int32.TryParse(strOptCountQuotaPerDay, out optCountQuotaPerDay))
            {
                optCountQuotaPerDay = 10000;
            }
            this.objectMetadata_CacheControl = config.ContainsKey("objectMetadata_CacheControl") ? config["objectMetadata_CacheControl"] : "";
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="objectName"></param>
        /// <returns></returns>
        public ObjectWrapper GetObject(string objectName)
        {
            if (objectName != null)
            {
                objectName = objectName.Trim('/');
            }
            if (String.IsNullOrEmpty(objectName))
            {
                throw new ArgumentNullException("objectName");
            }
            try
            {
                if (client.DoesObjectExist(this.bucketName, objectName))
                {
                    var obj = client.GetObject(this.bucketName, objectName);
                    var objectMetadata = new ObjectMetadata();
                    objectMetadata = mapOssMetadataToObjectMetadata(obj.Metadata, objectMetadata);
                    return new ObjectWrapper(objectName, obj.Content, objectMetadata);
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

        /// <summary>
        /// 
        /// </summary>
        /// <param name="objectName"></param>
        /// <returns></returns>
        public ObjectMetadata GetObjectMetadata(string objectName)
        {
            if (objectName != null)
            {
                objectName = objectName.Trim('/');
            }
            if (String.IsNullOrEmpty(objectName))
            {
                throw new ArgumentNullException("objectName");
            }
            try
            {
                if (client.DoesObjectExist(this.bucketName, objectName))
                {
                    var ossMetadata = client.GetObjectMetadata(this.bucketName, objectName);
                    var objectMetadata = new ObjectMetadata();
                    objectMetadata = mapOssMetadataToObjectMetadata(ossMetadata, objectMetadata);
                    return objectMetadata;
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
                throw new StorageIOException("Get object metadata fail!", ex);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="objectName"></param>
        /// <param name="objectMetadata"></param>
        public void SetObjectMetadata(string objectName, ObjectMetadata objectMetadata)
        {
            if (objectName != null)
            {
                objectName = objectName.Trim('/');
            }
            if (String.IsNullOrEmpty(objectName))
            {
                throw new ArgumentNullException("objectName");
            }
            if (objectMetadata == null)
            {
                throw new ArgumentNullException("objectMetadata");
            }
            try
            {
                if (client.DoesObjectExist(this.bucketName, objectName))
                {
                    var ossMetadata = buildOssMetadata(objectName, objectMetadata);
                    ossMetadata = mapObjectMetadataToOssMetadata(objectMetadata, ossMetadata);
                    client.ModifyObjectMeta(this.bucketName, objectName, ossMetadata);
                }
            }
            catch (Exception ex)
            {
                throw new StorageIOException("Set object metadata fail!", ex);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="objectName"></param>
        /// <param name="content"></param>
        /// <param name="overrideIfExists"></param>
        /// <param name="objectMetadata"></param>
        public void PutObject(string objectName, Stream content, bool overrideIfExists, ObjectMetadata objectMetadata = null)
        {
            if (objectName != null)
            {
                objectName = objectName.Trim('/');
            }
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
                if (!client.DoesBucketExist(this.bucketName))
                {
                    client.CreateBucket(this.bucketName);
                }
                bool objExists = client.DoesObjectExist(this.bucketName, objectName);
                if (overrideIfExists || !objExists)
                {
                    var metadata = buildOssMetadata(objectName, objectMetadata);
                    client.PutObject(this.bucketName, objectName, content, metadata);
                }
            }
            catch (Exception ex)
            {
                throw new StorageIOException("Put object fail!", ex);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="objectName"></param>
        public void DeleteObject(string objectName)
        {
            if (objectName != null)
            {
                objectName = objectName.Trim('/');
            }
            if (String.IsNullOrEmpty(objectName))
            {
                throw new ArgumentNullException("objectName");
            }
            try
            {
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

        /// <summary>
        /// 
        /// </summary>
        /// <param name="objectName"></param>
        /// <returns></returns>
        public bool ObjectExists(string objectName)
        {
            if (objectName != null)
            {
                objectName = objectName.Trim('/');
            }
            if (String.IsNullOrEmpty(objectName))
            {
                throw new ArgumentNullException("objectName");
            }
            try
            {
                return client.DoesObjectExist(this.bucketName, objectName);
            }
            catch (Exception ex)
            {
                throw new StorageIOException("Check object exists or not fail!", ex);
            }
        }

        private Aliyun.OSS.ObjectMetadata buildOssMetadata(string objectName, ObjectMetadata objectMetadata)
        {
            Aliyun.OSS.ObjectMetadata ossMetadata = new Aliyun.OSS.ObjectMetadata();
            if (objectMetadata != null)
            {
                if (String.IsNullOrEmpty(objectMetadata.ContentType))
                {
                    objectMetadata.ContentType = MimeUtils.Instance.GetMimeByFileExt(Path.GetExtension(objectName));
                }
                ossMetadata = mapObjectMetadataToOssMetadata(objectMetadata, ossMetadata);
            }
            if (!String.IsNullOrEmpty(this.objectMetadata_CacheControl))
            {
                ossMetadata.CacheControl = objectMetadata_CacheControl;
            }
            return ossMetadata;
        }

        private ObjectMetadata mapOssMetadataToObjectMetadata(Aliyun.OSS.ObjectMetadata ossMetadata, ObjectMetadata objectMetadata)
        {
            if (ossMetadata != null && objectMetadata != null)
            {
                objectMetadata.ContentDisposition = ossMetadata.ContentDisposition;
                if (ossMetadata.ContentLength >= 0)
                {
                    objectMetadata.ContentLength = ossMetadata.ContentLength;
                }
                if (!String.IsNullOrEmpty(ossMetadata.ContentType))
                {
                    objectMetadata.ContentType = ossMetadata.ContentType;
                }

                foreach (var userMetadata in ossMetadata.UserMetadata)
                {
                    if (objectMetadata.UserMetadata.ContainsKey(userMetadata.Key))
                    {
                        objectMetadata.UserMetadata[userMetadata.Key] = userMetadata.Value;
                    }
                    else
                    {
                        objectMetadata.UserMetadata.Add(userMetadata.Key, userMetadata.Value);
                    }
                }
            }
            return objectMetadata;
        }

        private Aliyun.OSS.ObjectMetadata mapObjectMetadataToOssMetadata(ObjectMetadata objectMetadata, Aliyun.OSS.ObjectMetadata ossMetadata)
        {
            if (ossMetadata != null && objectMetadata != null)
            {
                ossMetadata.ContentDisposition = objectMetadata.ContentDisposition;
                if (!String.IsNullOrEmpty(objectMetadata.ContentType))
                {
                    ossMetadata.ContentType = objectMetadata.ContentType;
                }

                foreach (var userMetadata in objectMetadata.UserMetadata)
                {
                    if (ossMetadata.UserMetadata.ContainsKey(userMetadata.Key))
                    {
                        ossMetadata.UserMetadata[userMetadata.Key] = userMetadata.Value;
                    }
                    else
                    {
                        ossMetadata.UserMetadata.Add(userMetadata.Key, userMetadata.Value);
                    }
                }
            }
            return ossMetadata;
        }
    }
}
