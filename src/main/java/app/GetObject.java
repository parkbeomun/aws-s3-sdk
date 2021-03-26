package app;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.SdkClientException;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ResponseHeaderOverrides;
import com.amazonaws.services.s3.model.S3Object;
import software.amazon.awssdk.core.SdkClient;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

public class GetObject {

    public static void main(String[] args) throws IOException {

        Regions clientRegion = Regions.DEFAULT_REGION;
        String bucketName = "";
        String key = "";

        S3Object fullObject = null, objectPortion = null, headerOvverideObject = null;

        try {

            AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
                    .withRegion(clientRegion)
                    .withCredentials(new ProfileCredentialsProvider())
                    .build();


            // Get an object and print its contents.
            System.out.println("Download on object");
            fullObject = s3Client.getObject(new GetObjectRequest(bucketName, key));
            System.out.println("Content-Type: " + fullObject.getObjectMetadata().getContentType());
            System.out.println("Content: ");
            displaytTextINputStream(fullObject.getObjectContent());

            //Get a range of bytes from an object print the bytes.
            GetObjectRequest rangeObjectRequest = new GetObjectRequest(bucketName, key)
                    .withRange(0, 9);
            objectPortion = s3Client.getObject(rangeObjectRequest);
            System.out.println("printing bytes retrieved.");
            displaytTextINputStream(objectPortion.getObjectContent());

            //Get an entire object, overriding the speccified response heaers, and print the object's content
            ResponseHeaderOverrides headerOverrides = new ResponseHeaderOverrides()
                    .withCacheControl("No-cache")
                    .withContentDisposition("attachment; filename=example.txt");

            GetObjectRequest getObjectRequestHeaderOverride = new GetObjectRequest(bucketName, key)
                    .withResponseHeaders(headerOverrides);
            headerOvverideObject = s3Client.getObject(getObjectRequestHeaderOverride);
            displaytTextINputStream(headerOvverideObject.getObjectContent());

        } catch (AmazonServiceException ase) {

            //The call was transmitted successfully, but Amazon S3 couldn't process
            //it, so it returned an error response.
            ase.printStackTrace();

        } catch (SdkClientException se) {

            //Amazon S3 Couldn't contacted for a response, or the client
            //couldn't parse the response from Amazon S3.
            se.printStackTrace();
        } finally {
            // To ensure that the network connection doesen't remain open, close any open input stream.
            if (fullObject != null) {
                fullObject.close();
            }
            if (objectPortion != null) {
                objectPortion.close();
            }
            if (headerOvverideObject != null) {
                headerOvverideObject.close();
            }
        }

    }

    private static void displaytTextINputStream(InputStream is) throws IOException{

        //Read the text input stream one line at a time and display each line.
        BufferedReader br = new BufferedReader(new InputStreamReader(is));
        String line = null;
        while((line = br.readLine()) != null) {
            System.out.println(line);
        }
        System.out.println();

    }
}
