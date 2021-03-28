package app;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.SdkClientException;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;
import com.amazonaws.services.s3.transfer.Upload;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;

//@TODO  No content length specified for stream data.  Stream contents will be buffered in memory and could result in out of memory errors. 에러체크

public class HighLevelMultipartUploadByInputStream {

    public static void main(String[] args) throws Exception {
        Regions clientRegion = Regions.AP_NORTHEAST_2;
        String bucketName = "";
        String keyName = ""; //키네임은 파일 시퀀스 (pk) 로 하면 될듯
        String filePath = "";

        try {



            AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
                    .withRegion(clientRegion)
                    .withCredentials(new ProfileCredentialsProvider())
                    .build();
            TransferManager tm = TransferManagerBuilder.standard()
                    .withS3Client(s3Client)
                    .build();

            File file = new File(filePath);
            byte[] bytes = Files.readAllBytes(file.toPath());
            ObjectMetadata objectMetadata = new ObjectMetadata();
            objectMetadata.setContentType(getMediaTypeByFileName(file.getName()));
            objectMetadata.setContentLength(bytes.length);
            Upload upload = tm.upload(bucketName, keyName, new ByteArrayInputStream(bytes), objectMetadata);

            System.out.println("Object upload started");

            //Optionally, wait for the upload to finish before continuing.
            upload.waitForCompletion();
            System.out.println("Object upload complete");

        } catch (AmazonServiceException e) {
            e.printStackTrace();
        } catch (SdkClientException e) {
            e.printStackTrace();
        }

    }

    public static String getMediaTypeByFileName(String fileName) {

        int pos = fileName.lastIndexOf(".");
        String fileExt = fileName.substring(pos + 1).toLowerCase();

        if (fileExt.equals("avi")) {
            return "video/x-msvideo";
        } else if (fileExt.equals("doc")) {
            return "application/msword";
        } else if (fileExt.equals("gif")) {
            return "image/gif";
        } else if (fileExt.equals("jpeg") || fileExt.equals("jpg")) {
            return "image/jpeg";
        }else if(fileExt.equals("png")) {
            return "image/png";
        }else if(fileExt.equals("mpeg")) {
            return "video/mpeg";
        }else if(fileExt.equals("pdf")) {
            return "application/pdf";
        }else if(fileExt.equals("ppt")) {
            return "application/vnd.ms-powerpoint";
        }else if(fileExt.equals("zip")) {
            return "application/zip";
        }else if(fileExt.equals("7z")) {
            return "application/x-7z-compressed";
        }

        return "";
    }
}
