package elastic.cara.rds.util;

import javax.crypto.Cipher;
import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import javax.security.auth.x500.X500Principal;
import java.io.FileInputStream;
import java.security.KeyStore;
import java.security.NoSuchAlgorithmException;
import java.security.Security;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.util.Base64;

public class KeyGeneratorUtility {

//    private static final String ALGORITHM = "AES";
//    private static final String TRANSFORMATION = "AES";
//
//    public static String encrypt(String text, SecretKey key) throws Exception {
//        Cipher cipher = Cipher.getInstance(TRANSFORMATION);
//        cipher.init(Cipher.ENCRYPT_MODE, key);
//        byte[] encryptedBytes = cipher.doFinal(text.getBytes());
//        return Base64.getEncoder().encodeToString(encryptedBytes);
//    }
//
//    public static String decrypt(String encryptedText, SecretKey key) throws Exception {
//        Cipher cipher = Cipher.getInstance(TRANSFORMATION);
//        cipher.init(Cipher.DECRYPT_MODE, key);
//        byte[] decodedBytes = Base64.getDecoder().decode(encryptedText);
//        byte[] decryptedBytes = cipher.doFinal(decodedBytes);
//        return new String(decryptedBytes);
//    }
//
//    public static SecretKey generateKey() throws Exception {
//        KeyGenerator keyGenerator = KeyGenerator.getInstance(ALGORITHM);
//        keyGenerator.init(128);
//        return keyGenerator.generateKey();
//    }
//
//    public static SecretKey decodeKey(String keyStr) {
//        byte[] decodedKey = Base64.getDecoder().decode(keyStr);
//        return new SecretKeySpec(decodedKey, 0, decodedKey.length, ALGORITHM);
//    }
//
//    public static String encodeKey(SecretKey key) {
//        return Base64.getEncoder().encodeToString(key.getEncoded());
//    }

//    public static void main(String[] args) throws Exception {
//        SecretKey key = generateKey();
//        String encodedString = encodeKey(key);
//        System.out.println("encodedString=> "+encodedString);
//        key = decodeKey(encodedString);
//        String token = "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJzdmNfcmRzfFJEUyBSZWJ1aWxkIiwidCI6InMiLCJjcmVhdGVkIjoiMjAyNC0wOC0wMVQwNTo1Nzo0NS45OTkzMDYxMjRaIn0.FoeTjr5d9XHySwCu3Th5ry_O7ivyu4qGgBAyQaPdsX4";
//        String encryptedToken = encrypt(token, key);
//        System.out.println("Encrypted Token: " + encryptedToken);
//
//        String decryptedToken = decrypt(encryptedToken, key);
//        System.out.println("Decrypted Token: " + decryptedToken);
//        if(token.contentEquals(decryptedToken))
//            System.out.println("Token same");
//        else
//            System.out.println("Token not same");
//    }

//    public static void main(String[] args) {
//        try {
//            // Add BouncyCastle Provider
//            //Security.addProvider(new BouncyCastleProvider());
//            // Path to the KeyStore file
//            String keystorePath = "C:\\Users\\a-VARGHMX1\\IntellijWS\\config\\mykeystore.jks";
//            // Password for the KeyStore
//            String keystorePassword = "mykeystorepassword";
//            // Alias for the entry
//            String alias = "myalias";
//
//            // Load the KeyStore
//            KeyStore keyStore = KeyStore.getInstance("JKS");
//            try (FileInputStream fis = new FileInputStream(keystorePath)) {
//                keyStore.load(fis, keystorePassword.toCharArray());
//            }
//
//            // Retrieve the certificate
//            Certificate cert = keyStore.getCertificate(alias);
//            if (cert instanceof X509Certificate) {
//                X509Certificate x509Cert = (X509Certificate) cert;
//
//                // Extract the common name (CN) from the certificate subject
//                X500Principal principal = x509Cert.getSubjectX500Principal();
//                String subjectDN = principal.getName();
//                String token = getCommonName(subjectDN);
//
//                System.out.println("Token: " + token);
//            } else {
//                System.out.println("The certificate is not an instance of X509Certificate.");
//            }
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//    }
//
//    // Helper method to extract the common name (CN) from the subject DN
//    private static String getCommonName(String subjectDN) {
//        String[] dnComponents = subjectDN.split(",");
//        for (String component : dnComponents) {
//            if (component.trim().startsWith("CN=")) {
//                return component.trim().substring(3); // Remove "CN="
//            }
//        }
//        return null;
//    }


//    public static void main(String[] args) {
//        try {
//            // Add BouncyCastle Provider
//            Security.addProvider(new BouncyCastleProvider());
//            String token = "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJzdmNfcmRzfFJEUyBSZWJ1aWxkIiwidCI6InMiLCJjcmVhdGVkIjoiMjAyNC0wOC0wMVQwNTo1Nzo0NS45OTkzMDYxMjRaIn0.FoeTjr5d9XHySwCu3Th5ry_O7ivyu4qGgBAyQaPdsX4";
//            // Path to the KeyStore file
//            String keystorePath = "C:\\Users\\a-VARGHMX1\\IntellijWS\\config\\mykeystore.jks";
//            // Password for the KeyStore
//            String keystorePassword = "mykeystorepassword";
//            // Alias for the entry
//            String alias = "myalias";
//
//            // Load the KeyStore
//            KeyStore keyStore = KeyStore.getInstance("JKS");
//            try (FileInputStream fis = new FileInputStream(keystorePath)) {
//                keyStore.load(fis, keystorePassword.toCharArray());
//            }
//
//            // Retrieve the certificate
//            Certificate cert = keyStore.getCertificate(alias);
//            if (cert instanceof X509Certificate) {
//                X509Certificate x509Cert = (X509Certificate) cert;
//
//                // Retrieve the custom extension value
//                byte[] extensionValue = x509Cert.getExtensionValue("1.2.3.4.5.6.7.8");
//                if (extensionValue != null) {
//                    ASN1Primitive asn1Primitive = ASN1Primitive.fromByteArray(extensionValue);
//                    if (asn1Primitive instanceof ASN1OctetString) {
//                        ASN1OctetString octetString = (ASN1OctetString) asn1Primitive;
//                        String decryptedToken = new String(octetString.getOctets());
//                        System.out.println("Token: " + decryptedToken);
//                        if(token.contentEquals(decryptedToken))
//                            System.out.println("Token same");
//                        else
//                            System.out.println("Token not same");
//                    } else {
//                        System.out.println("Expected ASN1OctetString but found: " + asn1Primitive.getClass().getSimpleName());
//                    }
//                } else {
//                    System.out.println("Custom extension not found.");
//                }
//            } else {
//                System.out.println("The certificate is not an instance of X509Certificate.");
//            }
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//    }

//    public static void main(String[] args) {
//        try {
//            // Add BouncyCastle Provider
//            Security.addProvider(new BouncyCastleProvider());
//            String token = "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJzdmNfcmRzfFJEUyBSZWJ1aWxkIiwidCI6InMiLCJjcmVhdGVkIjoiMjAyNC0wOC0wMVQwNTo1Nzo0NS45OTkzMDYxMjRaIn0.FoeTjr5d9XHySwCu3Th5ry_O7ivyu4qGgBAyQaPdsX4";
//            // Path to the KeyStore file
//            String keystorePath = "C:\\Users\\a-VARGHMX1\\IntellijWS\\config\\mykeystore.jks";
//            // Password for the KeyStore
//            String keystorePassword = "mykeystorepassword";
//            // Alias for the entry
//            String alias = "myalias";
//
//            // Load the KeyStore
//            KeyStore keyStore = KeyStore.getInstance("JKS");
//            try (FileInputStream fis = new FileInputStream(keystorePath)) {
//                keyStore.load(fis, keystorePassword.toCharArray());
//            }
//
//            // Retrieve the certificate
//            Certificate cert = keyStore.getCertificate(alias);
//            if (cert instanceof X509Certificate) {
//                X509Certificate x509Cert = (X509Certificate) cert;
//
//                // Retrieve the custom extension value
//                ASN1ObjectIdentifier tokenOID = new ASN1ObjectIdentifier("1.2.3.4.5.6.7.8"); // Custom OID
//                byte[] extensionValue = x509Cert.getExtensionValue(tokenOID.getId());
//
//                if (extensionValue != null) {
//                    ASN1OctetString octetString = DEROctetString.getInstance(extensionValue);
//                    String decryptedToken = new String(octetString.getOctets(), "UTF-8");
//                    System.out.println("Token: " + decryptedToken);
//                    if(token.contentEquals(decryptedToken))
//                            System.out.println("Token same");
//                        else
//                            System.out.println("Token not same");
//                } else {
//                    System.out.println("Custom extension not found.");
//                }
//            } else {
//                System.out.println("The certificate is not an instance of X509Certificate.");
//            }
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//    }
}


