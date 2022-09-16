package security;

import client.VngAuthenticationClient;

import javax.security.auth.Subject;
import javax.security.auth.callback.*;
import javax.security.auth.login.LoginException;
import javax.security.auth.spi.LoginModule;
import java.io.IOException;
import java.util.Map;

public class VNGLoginModule implements LoginModule {

    private VngAuthenticationClient vngAuthenticationClient;
    private CallbackHandler callbackHandler;

    @Override
    public void initialize(Subject subject, CallbackHandler callbackHandler, Map<String, ?> sharedState, Map<String, ?> options) {
        this.callbackHandler = callbackHandler;
        this.vngAuthenticationClient = new VngAuthenticationClient();
    }

    @Override
    public boolean login() throws LoginException {
        if (callbackHandler == null) {
            throw new LoginException("Oops, callbackHandler is null");
        }
        Callback[] callbacks = new Callback[2];
        callbacks[0] = new NameCallback("name");
        callbacks[1] = new PasswordCallback("password", false);
        try {
            callbackHandler.handle(callbacks);
        } catch (IOException e) {
            throw new LoginException("Oops, IOException calling handle on callbackHandler");
        } catch (UnsupportedCallbackException e) {
            throw new LoginException("Oops, UnsupportedCallbackException calling handle on callbackHandler");
        }

        NameCallback nameCallback = (NameCallback) callbacks[0];
        PasswordCallback passwordCallback = (PasswordCallback) callbacks[1];

        String allowedCredentials = "datahub";
        String name = nameCallback.getName();
        String password = new String(passwordCallback.getPassword());
        if (name.equals(allowedCredentials) && password.equals(allowedCredentials)) {
            return true;
        }
        try {
            final VngAuthenticationClient.Response vngAuthResponse = vngAuthenticationClient.authenticate(name, password);
            return vngAuthResponse.getStatus();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return false;
    }

    @Override
    public boolean commit() throws LoginException {
        return true;
    }

    @Override
    public boolean abort() throws LoginException {
        return true;
    }

    @Override
    public boolean logout() throws LoginException {
        return true;
    }
}