package com.psddev.dari.util;

import com.google.common.collect.ImmutableMap;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.jvnet.mock_javamail.Mailbox;
import org.mockito.runners.MockitoJUnitRunner;

import javax.mail.Message;
import javax.mail.MessagingException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class SmtpMailProviderTest {

    @Test
    public void testInitialize() throws IOException {
        SmtpMailProvider provider = new SmtpMailProvider();
        provider.initialize("smtp", (Map) new ImmutableMap.Builder<>()
                .put("class", SmtpMailProvider.class.getName())
                .put("host", "localhost")
                .put("port", 25)
                .put("username", "username")
                .put("password", "password")
                .put("useTls", "true")
                .put("tlsPort", 325)
                .put("useSsl", "true")
                .put("sslPort", 325)
                .build());
        assertEquals("localhost", provider.getHost());
        assertEquals(25, provider.getPort());
        assertEquals("username", provider.getUsername());
        assertEquals("password", provider.getPassword());
        assert(provider.isUseTls());
        assertEquals(325, provider.getTlsPort());
        assert(provider.isUseSsl());
        assertEquals(325, provider.getSslPort());
    }

    @Test(expected = IllegalArgumentException.class)
    public void nullHostSend() throws IOException {
        SmtpMailProvider mailProvider  = new SmtpMailProvider();
        MailMessage message = mock(MailMessage.class);
        mailProvider.setHost(null);
        mailProvider.send(message);
    }

    @Test(expected = IllegalArgumentException.class)
    public void nullMessageSend() throws IOException {
        SmtpMailProvider mailProvider  = new SmtpMailProvider();
        mailProvider.setHost("localhost");
        mailProvider.send(null);
    }

    @Test
    public void testSend() throws IOException, MessagingException {
        SmtpMailProvider mailProvider  = getProvider();
        mailProvider.setUseTls(true);
        mailProvider.setTlsPort(325);
        MailMessage message = mock(MailMessage.class);
        when(message.getTo()).thenReturn("test@example.com");
        when(message.getFrom()).thenReturn("example@test.com");
        when(message.getBodyPlain()).thenReturn("This is my body");
        when(message.getReplyTo()).thenReturn("return@test.com");
        mailProvider.send(message);
        List<Message> inbox = Mailbox.get("test@example.com");
        assertEquals(1, inbox.size());
        Mailbox.clearAll();
    }

    @Test
    public void testSendSsl() throws IOException, MessagingException {
        SmtpMailProvider mailProvider  = getProvider();
        mailProvider.setUseSsl(true);
        mailProvider.setSslPort(325);
        MailMessage message = mock(MailMessage.class);
        when(message.getTo()).thenReturn("test@example.com");
        when(message.getFrom()).thenReturn("example@test.com");
        when(message.getBodyPlain()).thenReturn("This is my body");
        when(message.getReplyTo()).thenReturn("return@test.com");
        mailProvider.send(message);
        List<Message> inbox = Mailbox.get("test@example.com");
        assertEquals(1, inbox.size());
        Mailbox.clearAll();
    }

    @Test(expected = RuntimeException.class)
    public void testSendFail() throws IOException, MessagingException {
        SmtpMailProvider mailProvider  = getProvider();
        mailProvider.setUseTls(true);
        mailProvider.setTlsPort(325);
        MailMessage message = mock(MailMessage.class);
        when(message.getTo()).thenReturn("test   .com");
        when(message.getFrom()).thenReturn("example@test.com");
        when(message.getBodyPlain()).thenReturn("This is my body");
        mailProvider.send(message);
        List<Message> inbox = Mailbox.get("test@example.com");
        assertEquals(1, inbox.size());
        Mailbox.clearAll();
    }

    private SmtpMailProvider getProvider() {
        SmtpMailProvider mailProvider  = new SmtpMailProvider();
        mailProvider.setHost("localhost");
        mailProvider.setUsername("test");
        mailProvider.setPassword("password");
        mailProvider.setPort(25);
        return mailProvider;
    }

    @Test
    public void nullHostSendBulk() throws IOException, MessagingException {
        SmtpMailProvider mailProvider = new SmtpMailProvider();
        MailMessage message = mock(MailMessage.class);
        mailProvider.setHost(null);
        mailProvider.sendBulk(Collections.singletonList(message), new MailProviderCallbackHandler() {
            @Override
            public void onSuccess(MailMessage message) {
                fail();
            }

            @Override
            public void onFail(MailMessage message, Exception exception) {
                assert (exception instanceof IllegalArgumentException);
            }
        });
    }

    @Test
    public void nullMessageSendBulk() throws IOException {
        SmtpMailProvider mailProvider  = new SmtpMailProvider();
        mailProvider.setHost("localhost");
        mailProvider.sendBulk(null, new MailProviderCallbackHandler() {
            @Override
            public void onSuccess(MailMessage message) {
                fail();
            }

            @Override
            public void onFail(MailMessage message, Exception exception) {
                assert (exception instanceof IllegalArgumentException);
            }
        });
    }

    @Test
    public void testSendBulkSuccess() throws IOException, MessagingException {
        SmtpMailProvider mailProvider  = getProvider();
        MailMessage messageSuccess = mock(MailMessage.class);
        when(messageSuccess.getTo()).thenReturn("test@example.com");
        when(messageSuccess.getFrom()).thenReturn("example@test.com");
        when(messageSuccess.getBodyHtml()).thenReturn("This is my body html");
        List<MailMessage> messages = new ArrayList<>();
        messages.add(messageSuccess);
        messages.add(messageSuccess);
        messages.add(messageSuccess);
        mailProvider.sendBulk(messages, new MailProviderCallbackHandler() {
            @Override
            public void onSuccess(MailMessage message) {
                assertEquals("test@example.com", message.getTo());
            }

            @Override
            public void onFail(MailMessage message, Exception exception) {
                fail();
            }
        });
        List<Message> inbox = Mailbox.get("test@example.com");
        assertEquals(3, inbox.size());
        Mailbox.clearAll();
    }

    @Test
    public void testSendBulkFail() throws IOException, MessagingException {
        SmtpMailProvider mailProvider  = getProvider();
        MailMessage messageFail = mock(MailMessage.class);
        when(messageFail.getTo()).thenReturn("test@  example.com");
        when(messageFail.getFrom()).thenReturn("example@test.com");
        when(messageFail.getBodyPlain()).thenReturn("This is my body");
        List<MailMessage> messages = new ArrayList<>();
        messages.add(messageFail);
        messages.add(null);
        mailProvider.sendBulk(messages, new MailProviderCallbackHandler() {
            @Override
            public void onSuccess(MailMessage message) {
                fail();
            }

            @Override
            public void onFail(MailMessage message, Exception exception) {
                if (message != null) {
                    assertEquals("test@  example.com", message.getTo());
                } else {
                    assert (exception instanceof NullPointerException);
                }
            }
        });
        List<Message> inbox = Mailbox.get("test@example.com");
        assertEquals(0, inbox.size());
        Mailbox.clearAll();
    }
}
