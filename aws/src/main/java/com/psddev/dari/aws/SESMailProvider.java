package com.psddev.dari.aws;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.simpleemail.AmazonSimpleEmailServiceClient;
import com.amazonaws.services.simpleemail.model.RawMessage;
import com.amazonaws.services.simpleemail.model.SendRawEmailRequest;
import com.psddev.dari.util.AbstractMailProvider;
import com.psddev.dari.util.MailMessage;
import com.psddev.dari.util.MailProviderCallbackHandler;
import com.psddev.dari.util.ObjectUtils;
import com.psddev.dari.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.mail.MessagingException;
import javax.mail.Session;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeBodyPart;
import javax.mail.internet.MimeMessage;
import javax.mail.internet.MimeMultipart;
import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class SESMailProvider extends AbstractMailProvider {

    /**
     * Sub-setting key for SES access key.
     */
    public static final String ACCESS_SUB_SETTING = "access";

    /**
     * Sub-setting key for SES secret access key.
     */
    public static final String SECRET_SUB_SETTING = "secret";

    private static final Logger LOGGER = LoggerFactory.getLogger(com.psddev.dari.aws.SESMailProvider.class);

    private String secret;
    private String access;

    public String getSecret() {
        return secret;
    }

    public void setSecret(String secret) {
        this.secret = secret;
    }

    public String getAccess() {
        return access;
    }

    public void setAccess(String access) {
        this.access = access;
    }

    @Override
    public void send(MailMessage message) {
        if (message == null) {
            String errorText = "EmailMessage can't be null!";
            LOGGER.error(errorText);
            throw new IllegalArgumentException(errorText);
        }
        Session session = Session.getDefaultInstance(new Properties());
        try {
            MimeMessage mimeMessage = createMimeMessage(session, message);
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            mimeMessage.writeTo(outputStream);
            RawMessage rawMessage = new RawMessage(ByteBuffer.wrap(outputStream.toByteArray()));
            SendRawEmailRequest rawEmailRequest = new SendRawEmailRequest(rawMessage);
            createClient().sendRawEmail(rawEmailRequest);
        } catch (Exception e) {
            LOGGER.warn("Failed to send: [{}]", e.getMessage());
            throw new RuntimeException(e);
        }
    }

    @Override
    public void sendBulk(List<MailMessage> messages, MailProviderCallbackHandler callback) {
        if (messages == null) {
            String errorText = "Messages can't be null!";
            LOGGER.error(errorText);
            callback.onFail(null, new IllegalArgumentException(errorText));
            return;
        }
        AmazonSimpleEmailServiceClient client = createClient();
        Session session = Session.getDefaultInstance(new Properties());
        for (MailMessage message : messages) {
            if (message == null) {
                String errorText = "Message can't be null!";
                LOGGER.error(errorText);
                callback.onFail(message, new IllegalArgumentException(errorText));
            } else {
                try {
                    MimeMessage mimeMessage = createMimeMessage(session, message);
                    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
                    mimeMessage.writeTo(outputStream);
                    RawMessage rawMessage = new RawMessage(ByteBuffer.wrap(outputStream.toByteArray()));
                    SendRawEmailRequest rawEmailRequest = new SendRawEmailRequest(rawMessage);
                    client.sendRawEmail(rawEmailRequest);
                    callback.onSuccess(message);
                } catch (Exception e) {
                    LOGGER.warn("Failed to send: [{}]", e.getMessage());
                    callback.onFail(message, e);
                }
            }
        }
    }

    private AmazonSimpleEmailServiceClient createClient() {
        String access = getAccess();
        String secret = getSecret();

        AmazonSimpleEmailServiceClient client =  !ObjectUtils.isBlank(access) && !ObjectUtils.isBlank(secret)
                ? new AmazonSimpleEmailServiceClient(new BasicAWSCredentials(access, secret))
                : new AmazonSimpleEmailServiceClient(new DefaultAWSCredentialsProviderChain());
        return client;
    }

    private MimeMessage createMimeMessage(Session session, MailMessage message) throws MessagingException {
        MimeMessage mimeMessage = new MimeMessage(session);
        mimeMessage.setSubject(message.getSubject(), "UTF-8");

        mimeMessage.setFrom(new InternetAddress(message.getFrom()));
        mimeMessage.setRecipients(javax.mail.Message.RecipientType.TO, InternetAddress.parse(message.getTo()));
        mimeMessage.setSubject(message.getSubject());

        if (!StringUtils.isEmpty(message.getReplyTo())) {
            mimeMessage.setReplyTo(InternetAddress.parse(message.getReplyTo()));
        }

        // Body, plain vs. html
        MimeMultipart multiPartContent = new MimeMultipart();
        if (!StringUtils.isEmpty(message.getBodyPlain())) {
            MimeBodyPart plain = new MimeBodyPart();
            plain.setText(message.getBodyPlain(), StandardCharsets.UTF_8.toString());
            multiPartContent.addBodyPart(plain);
        }
        if (!StringUtils.isEmpty(message.getBodyHtml())) {
            MimeBodyPart html = new MimeBodyPart();
            html.setContent(message.getBodyHtml(), "text/html; charset=" + StandardCharsets.UTF_8.toString());
            multiPartContent.addBodyPart(html);
            multiPartContent.setSubType("alternative");
        }
        mimeMessage.setContent(multiPartContent);
        return mimeMessage;
    }

    @Override
    public void initialize(String settingsKey, Map<String, Object> settings) {
        setAccess(ObjectUtils.to(String.class, settings.get(ACCESS_SUB_SETTING)));
        setSecret(ObjectUtils.to(String.class, settings.get(SECRET_SUB_SETTING)));
    }
}
