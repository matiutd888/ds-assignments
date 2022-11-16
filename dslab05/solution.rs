use hmac::{Hmac, Mac};
use sha2::Sha256;
use std::io::{Read, Write};
// You can add here other imports from std or crates listed in Cargo.toml.

// The below `PhantomData` marker is here only to suppress the "unused type
// parameter" error. Remove it when you implement your solution:

use std::sync::Arc;

use rustls::{ClientConnection, RootCertStore, ServerConnection, StreamOwned};

pub struct SecureClient<L: Read + Write> {
    streamowned: StreamOwned<ClientConnection, L>,
    hmac_key: Vec<u8>,
}

pub struct SecureServer<L: Read + Write> {
    streamowned: StreamOwned<ServerConnection, L>,
    hmac_key: Vec<u8>,
}

impl<L: Read + Write> SecureClient<L> {
    /// Creates a new instance of SecureClient.
    ///
    /// SecureClient communicates with SecureServer via `link`.
    /// The messages include a HMAC tag calculated using `hmac_key`.
    /// A certificate of SecureServer is signed by `root_cert`.
    pub fn new(link: L, hmac_key: &[u8], root_cert: &str) -> Self {
        let mut root_store = RootCertStore::empty();

        // Add to the store the root certificate of the server:
        root_store
            .add_parsable_certificates(&rustls_pemfile::certs(&mut root_cert.as_bytes()).unwrap());

        let client_config = rustls::ClientConfig::builder()
            .with_safe_defaults()
            .with_root_certificates(root_store)
            .with_no_client_auth();

        let connection =
            ClientConnection::new(Arc::new(client_config), "localhost".try_into().unwrap())
                .unwrap();

        let streamowned: StreamOwned<ClientConnection, L> =
            rustls::StreamOwned::new(connection, link);
        let hmac_vec: Vec<u8> = hmac_key.to_vec();
        SecureClient {
            streamowned: streamowned,
            hmac_key: hmac_vec,
        }
    }

    fn calculate_hmac_tag(data: &[u8], secret_key: &[u8]) -> [u8; 32] {
        // Initialize a new MAC instance from the secret key:
        let mut mac = Hmac::<Sha256>::new_from_slice(secret_key).unwrap();

        // Calculate MAC for the data (one can provide it in multiple portions):
        mac.update(data);

        // Finalize the computations of MAC and obtain the resulting tag:
        let tag = mac.finalize().into_bytes();

        tag.into()
    }

    /// Sends the data to the server. The sent message follows the
    /// format specified in the description of the assignment.
    pub fn send_msg(&mut self, data: Vec<u8>) {
        let length_in_network_bytes: [u8; 4] = (data.len() as u32).to_be_bytes();
        let hmac_tag = Self::calculate_hmac_tag(&data, &self.hmac_key);
        self.streamowned
            .write_all(&length_in_network_bytes)
            .unwrap();
        self.streamowned.write_all(&data).unwrap();
        self.streamowned.write_all(&hmac_tag).unwrap();
    }
}

impl<L: Read + Write> SecureServer<L> {
    /// Creates a new instance of SecureServer.
    ///
    /// SecureServer receives messages from SecureClients via `link`.
    /// HMAC tags of the messages are verified against `hmac_key`.
    /// The private key of the SecureServer's certificate is `server_private_key`,
    /// and the full certificate chain is `server_full_chain`.
    pub fn new(
        link: L,
        hmac_key: &[u8],
        server_private_key: &str,
        server_full_chain: &str,
    ) -> Self {
        let certs = rustls_pemfile::certs(&mut server_full_chain.as_bytes())
            .unwrap()
            .iter()
            .map(|v| rustls::Certificate(v.clone()))
            .collect();

        // Load the private key for the server (for simplicity, we assume there is
        // provided one valid key and it is a RSA private key):
        let private_key = rustls::PrivateKey(
            rustls_pemfile::rsa_private_keys(&mut server_private_key.as_bytes())
                .unwrap()
                .first()
                .unwrap()
                .to_vec(),
        );

        // Create a TLS configuration for the server:
        let server_config = rustls::ServerConfig::builder()
            .with_safe_defaults()
            .with_no_client_auth()
            .with_single_cert(certs, private_key)
            .unwrap();
        // Create a TLS connection using the configuration prepared above:
        let connection = rustls::ServerConnection::new(Arc::new(server_config)).unwrap();

        SecureServer {
            streamowned: StreamOwned::new(connection, link),
            hmac_key: hmac_key.to_vec(),
        }
    }

    fn verify_hmac_tag(tag: &[u8], message: &[u8], secret_key: &[u8]) -> bool {
        // Initialize a new MAC instance from the secret key:
        let mut mac = Hmac::<Sha256>::new_from_slice(secret_key).unwrap();

        // Calculate MAC for the data (one can provide it in multiple portions):
        mac.update(message);

        // Verify the tag:
        mac.verify_slice(tag).is_ok()
    }

    /// Receives the next incoming message and returns the message's content
    /// (i.e., without the message size and without the HMAC tag) if the
    /// message's HMAC tag is correct. Otherwise returns `SecureServerError`.
    pub fn recv_message(&mut self) -> Result<Vec<u8>, SecureServerError> {
        let mut length_bytes: [u8; 4] = [0; 4];
        self.streamowned.read_exact(length_bytes.as_mut()).unwrap();

        let length = u32::from_be_bytes(length_bytes);

        let mut message_bytes: Vec<u8> = vec![0; length as usize];

        self.streamowned.read_exact(message_bytes.as_mut()).unwrap();

        let mut hmac_bytes: [u8; 32] = [0; 32];
        self.streamowned.read_exact(hmac_bytes.as_mut()).unwrap();
        if Self::verify_hmac_tag(&hmac_bytes, &message_bytes, &self.hmac_key) {
            Ok(message_bytes.to_vec())
        } else {
            Err(SecureServerError::InvalidHmac)
        }
    }
}

#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub enum SecureServerError {
    /// The HMAC tag of a message is invalid.
    InvalidHmac,
}

// You can add any private types, structs, consts, functions, methods, etc., you need.
