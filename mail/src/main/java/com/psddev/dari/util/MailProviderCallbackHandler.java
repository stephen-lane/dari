package com.psddev.dari.util;

public interface MailProviderCallbackHandler {

    void onSuccess(MailMessage message);
    void onFail(MailMessage message, Exception exception);
}
