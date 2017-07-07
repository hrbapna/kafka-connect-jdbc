package io.confluent.connect.jdbc.source;

/**
 * Created by shawnvarghese on 6/7/17.
 */

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import javax.crypto.Mac;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.math.BigInteger;
import java.security.*;
import java.security.cert.CertificateException;
import org.apache.commons.codec.binary.Base64;

public class DataTransform implements Transform {

    private static final Logger log = LoggerFactory.getLogger(DataTransform.class);

    public String transformString(String value, String transformer) {
        String hashtext = null;
        if (value==null || value.trim().equals("")) {
            return value;
        }
        try {
            KeyStore ks = KeyStore.getInstance("JCEKS");

            // get user password and file input stream
            char[] password = "RedL0ck!Anon".toCharArray();

            java.io.FileInputStream fis = null;

            fis = new java.io.FileInputStream("ANON_KEYSTORE_FILE");
            ks.load(fis, password);
            KeyStore.ProtectionParameter protParam =
                    new KeyStore.PasswordProtection(password);
            fis.close();

            KeyStore.SecretKeyEntry secret = (KeyStore.SecretKeyEntry)ks.getEntry("anonymizeSKey",protParam);

            String message = value;

            Mac sha256_HMAC = Mac.getInstance("HmacSHA256");

            sha256_HMAC.init(secret.getSecretKey());

            String hash = Base64.encodeBase64String(sha256_HMAC.doFinal(message.getBytes()));

            hashtext = hash;

//            hashtext=value;
//            MessageDigest m = MessageDigest.getInstance("MD5");
//            if (value==null) {
//                return value;
//            }
//            m.update(value.getBytes());
//            byte[] digest = m.digest();
//            BigInteger bigInt = new BigInteger(1,digest);
//            hashtext = bigInt.toString(16);
//
//            while(hashtext.length() < 32 ){
//                hashtext = "0"+hashtext;
//            }
        } catch (NoSuchAlgorithmException e) {
            log.info("Hashing failed");
            e.printStackTrace();
        } catch (CertificateException e) {
            e.printStackTrace();
        } catch (KeyStoreException e) {
            e.printStackTrace();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (UnrecoverableEntryException e) {
            e.printStackTrace();
        } catch (InvalidKeyException e) {
            e.printStackTrace();
        }

        return hashtext;
    }
}
