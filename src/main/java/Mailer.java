
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.commons.mail.EmailException;
import org.apache.commons.mail.SimpleEmail;
import org.apache.log4j.Logger;

/**
 * Created with IntelliJ IDEA.
 * User: frahman
 * Date: 14/03/17
 * Time: 13:56
 * To change this template use File | Settings | File Templates.
 */
public class Mailer {
    private static Logger LOG = Logger.getLogger(Mailer.class);


    static boolean sendMailConfirms=System.getProperty("sendMailConfirms", "true").equalsIgnoreCase("true");
    private static String ADMIN_MAIL = System.getProperty("ADMIN_MAIL","mailfaz@gmail.com");
//    private static String ADMIN_MAIL = System.getProperty("ADMIN_MAIL","fazl.rahman@rat-allianz.com,alan.dalmon@rat-allianz.com");
    private static String MAILHOST   = System.getProperty("MAILHOST","ZH-SMTP-01.rat-allianz.com");
    private static String BOUNCE_RECIPIENT = System.getProperty("BOUNCE_RECIPIENT","fazl.rahman@rat-allianz.com");

    public static void sendMail(String subject, String body){
        sendMail(ADMIN_MAIL, subject, body);
    }

    public static void sendMail(String recipientCSV, String subject, String body){
        LOG.info("Mail subject: " + subject);
        SimpleEmail email = new SimpleEmail();
        email.setHostName(MAILHOST);
        try {
            // TODO test if you can loop over csv list of recips.. ?
            // todo test if it's faster to prepare a single static email object and just change subj/body each time ?
            for(String recipient : recipientCSV.split(",")){
                email.addTo(recipient, "Admin");
            }
            email.setBounceAddress(BOUNCE_RECIPIENT);
            email.setFrom("RDMInstaller@rat-allianz.com", "Grumpy RDM Installer");
            email.setSubject("[rms] " + new SimpleDateFormat("yyyyMMdd_HHmm").format(new Date()) + " " + subject);
            email.setMsg((body != null && !body.isEmpty()) ? body : " ");
            email.send();
        } catch (EmailException e) {
            LOG.warn("Failed sending mail;" + e.getMessage());
        }
    }

}
