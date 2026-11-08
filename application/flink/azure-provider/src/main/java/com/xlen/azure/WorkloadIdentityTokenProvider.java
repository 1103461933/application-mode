
package com.xlen.azure;

import com.azure.core.credential.AccessToken;
import com.azure.core.credential.TokenRequestContext;
import com.azure.identity.WorkloadIdentityCredential;
import com.azure.identity.WorkloadIdentityCredentialBuilder;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.azurebfs.extensions.CustomTokenProviderAdaptee;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.Date;

public class WorkloadIdentityTokenProvider
        implements CustomTokenProviderAdaptee {

    private static final String STORAGE_SCOPE =
            "https://storage.azure.com/.default";

    // Renew the token five minutes before expiration.
    private static final Duration REFRESH_BUFFER =
            Duration.ofMinutes(5);

    private WorkloadIdentityCredential credential;

    private volatile AccessToken accessToken;

    @Override
    public void initialize(
            Configuration configuration,
            String accountName) throws IOException {

        String clientId = requiredEnv("AZURE_CLIENT_ID");
        String tenantId = requiredEnv("AZURE_TENANT_ID");
        String tokenFile = requiredEnv("AZURE_FEDERATED_TOKEN_FILE");

        String authorityHost = System.getenv("AZURE_AUTHORITY_HOST");

        try {
            WorkloadIdentityCredentialBuilder builder =
                    new WorkloadIdentityCredentialBuilder()
                            .clientId(clientId)
                            .tenantId(tenantId)
                            .tokenFilePath(tokenFile);

            if (authorityHost != null && !authorityHost.isBlank()) {
                builder.authorityHost(authorityHost);
            }

            credential = builder.build();
            accessToken = null;

        } catch (Exception e) {
            throw new IOException(
                    "Failed to initialize Azure Workload Identity provider",
                    e);
        }
    }

    @Override
    public synchronized String getAccessToken() throws IOException {

        if (credential == null) {
            throw new IOException(
                    "Azure Workload Identity provider is not initialized");
        }

        AccessToken currentToken = accessToken;

        // Reuse the current token if it has enough validity remaining.
        if (isTokenValid(currentToken)) {
            return currentToken.getToken();
        }

        try {
            TokenRequestContext requestContext =
                    new TokenRequestContext()
                            .addScopes(STORAGE_SCOPE);

            AccessToken newToken = credential
                    .getToken(requestContext)
                    .block();

            if (newToken == null || newToken.getToken() == null
                    || newToken.getExpiresAt() == null) {
                throw new IOException(
                        "Azure Identity returned an invalid access token");
            }

            accessToken = newToken;

            return newToken.getToken();

        } catch (IOException e) {
            throw e;
        } catch (Exception e) {
            throw new IOException(
                    "Failed to acquire Azure Storage access token",
                    e);
        }
    }

    private boolean isTokenValid(AccessToken token) {

        if (token == null || token.getExpiresAt() == null) {
            return false;
        }

        Instant refreshThreshold =
                Instant.now().plus(REFRESH_BUFFER);

        return token.getExpiresAt().toInstant()
                .isAfter(refreshThreshold);
    }

    @Override
    public Date getExpiryTime() {

        AccessToken token = accessToken;

        if (token == null || token.getExpiresAt() == null) {
            return new Date(0);
        }

        return Date.from(token.getExpiresAt().toInstant());
    }

    private String requiredEnv(String name) throws IOException {

        String value = System.getenv(name);

        if (value == null || value.isBlank()) {
            throw new IOException(
                    "Required environment variable is missing: " + name);
        }

        return value;
    }
}
