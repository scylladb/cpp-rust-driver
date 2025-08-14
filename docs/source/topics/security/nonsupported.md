<!--
This file is purposely not part of the documentation tree,
as we do not want to advertise these features.
It is only left here to document the features that are not supported,
as this can be helpful if we want to implement them in the future:
- implementors can refer to this file to see what is not supported
  and how it is supposed to work;
- this can be moved to the documentation tree in the future
  if we decide to implement these features.
-->

```{eval-rst}
:orphan:
```

# Nonsupported security features

## Authentication

### Custom

A custom authentication implementation can be set using
`cass_cluster_set_authenticator_callbacks()`. This is useful for integrating
with more complex authentication systems such as Kerberos.

```c
typedef struct Credentials_ {
  const char* password;
  const char* username;
} Credentials;

void on_auth_initial(CassAuthenticator* auth, void* data) {
  /*
   * This callback is used to initiate a request to begin an authentication
   * exchange. Required resources can be acquired and initialized here.
   *
   * Resources required for this specific exchange can be stored in the
   * auth->data field and will be available in the subsequent challenge
   * and success phases of the exchange. The cleanup callback should be used to
   * free these resources.
   */

  /*
   * The data parameter contains the credentials passed in when the
   * authentication callbacks were set and is available to all
   * authentication exchanges.
   */
  const Credentials* credentials = (const Credentials *)data;

  size_t username_size = strlen(credentials->username);
  size_t password_size = strlen(credentials->password);
  size_t size = username_size + password_size + 2;

  /* Allocate a response token */
  char* response = cass_authenticator_response(auth, size);

  /* Credentials are prefixed with '\0' */
  response[0] = '\0';
  memcpy(response + 1, credentials->username, username_size);

  response[username_size + 1] = '\0';
  memcpy(response + username_size + 2, credentials->password, password_size);
}

void on_auth_challenge(CassAuthenticator* auth, void* data,
                       const char* token, size_t token_size) {
  /*
   * This is used for handling an authentication challenge initiated
   * by the server. The information contained in the token parameter is
   * authentication protocol specific. It may be NULL or empty.
   */
}

void on_auth_success(CassAuthenticator* auth, void* data,
                     const char* token, size_t token_size) {
  /*
   * This is to be used for handling the success phase of an exchange. The
   * token parameters contains information that may be used to finialize
   * the request. The information contained in the token parameter is
   * authentication protocol specific. It may be NULL or empty.
   */
}

void on_auth_cleanup(CassAuthenticator* auth, void* data) {
  /*
   * This is used to cleanup resources acquired during the authentication
   * exchange.
   */
}

int main() {
  CassCluster* cluster = cass_cluster_new();

  /* ... */

  /* Setup authentication callbacks and credentials */
  CassAuthenticatorCallbacks auth_callbacks = {
    on_auth_initial,
    on_auth_challenge,
    on_auth_success,
    on_auth_cleanup
  };

  /*
   * The `credentials` argument passed into `cass_cluster_set_auth_callbacks()`
   * is passed as the `data` parameter into the authentication callbacks.
   * Callbacks will be called by multiple threads concurrently so it is important
   * makes sure this data is either immutable or its access is serialized. The
   * `data` parameter can be cleaned up be passing a `CassAuthenticatorDataCleanupCallback`
   * to `cass_cluster_set_authenticator_callbacks()`.
   */
  Credentials credentials = {
    "cassandra",
    "cassandra"
  };

  /* Set custom authentication callbacks and credentials */
  cass_cluster_set_authenticator_callbacks(cluster,
                                           &auth_callbacks,
                                           NULL, /* No cleanup callback required */
                                           &credentials);

  /* ... */

  cass_cluster_free(cluster);
}
```
