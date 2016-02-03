package com.psddev.dari.util;

import com.google.common.collect.ImmutableMap;
import com.psddev.dari.aws.SESMailProvider;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SESMailProviderTest {

    @Test
    public void testInitialize() throws IOException {
        SESMailProvider provider = new SESMailProvider();
        provider.initialize("ses", (Map) new ImmutableMap.Builder<>()
                .put(SESMailProvider.ACCESS_SUB_SETTING, "ACCESS")
                .put(SESMailProvider.SECRET_SUB_SETTING, "SECRET")
                .build());
        assertEquals("ACCESS", provider.getAccess());
        assertEquals("SECRET", provider.getSecret());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNullMessageSend() throws IOException {
        SESMailProvider mailProvider  = new SESMailProvider();
        mailProvider.send(null);
    }

    @Test
    public void testNullMessageSendBulk() throws IOException {
        SESMailProvider mailProvider  = new SESMailProvider();
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

    /*
     *   Best test without having SES Credentials
     */
    @Test(expected = RuntimeException.class)
    public void testSend() {
        SESMailProvider mailProvider  = new SESMailProvider();
        mailProvider.setAccess("");
        mailProvider.setSecret("");
        MailMessage message = mock(MailMessage.class);
        when(message.getTo()).thenReturn("success@simulator.amazonses.com");
        when(message.getFrom()).thenReturn("example@test.com");
        when(message.getBodyPlain()).thenReturn("This is my body");
        when(message.getReplyTo()).thenReturn("return@test.com");

        mailProvider.send(message);
    }

    @Test
    public void sendBulk() {
        SESMailProvider mailProvider  = new SESMailProvider();
        mailProvider.setAccess("");
        mailProvider.setSecret("");
        MailMessage message = mock(MailMessage.class);
        when(message.getTo()).thenReturn("test@example.com");
        when(message.getFrom()).thenReturn("example@test.com");
        when(message.getBodyHtml()).thenReturn("This is my body html");
        List<MailMessage> messages = new ArrayList<>();
        messages.add(message);
        messages.add(null);
        messages.add(message);
        mailProvider.sendBulk(messages, new MailProviderCallbackHandler() {
            @Override
            public void onSuccess(MailMessage message) {
                // Should always fail because access and secret keys are not actual keys
            }

            @Override
            public void onFail(MailMessage message, Exception exception) {
                if (message != null) {
                    assert(exception instanceof RuntimeException);
                } else {
                    assert(exception instanceof NullPointerException);
                }
            }
        });
    }
}
