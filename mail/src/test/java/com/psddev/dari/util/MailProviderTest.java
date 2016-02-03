package com.psddev.dari.util;

import com.google.common.collect.ImmutableMap;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.IOException;

@RunWith(MockitoJUnitRunner.class)
public class MailProviderTest {

    @Test(expected = IllegalStateException.class)
    public void nullDefaultProvider() throws IOException {
        Settings.setOverride(MailProvider.DEFAULT_MAIL_SETTING, "");
        MailProvider.Static.getDefault();
    }

    @Test
    public void getDefault() {
        Settings.setOverride(MailProvider.DEFAULT_MAIL_SETTING, "smtp");
        Settings.setOverride(MailProvider.SETTING_PREFIX + "/smtp", ImmutableMap.of(
                "class", SmtpMailProvider.class.getName()
        ));
        MailProvider provider = MailProvider.Static.getDefault();
        assert(provider instanceof SmtpMailProvider);
    }
}