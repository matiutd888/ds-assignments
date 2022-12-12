// SystemCommandHeader

// TODO maybe all messages should have separate, web interfaces. 
// That way we could use deser to deserialize them.

struct ClientProcess {
    magic_number: u64,
    padding: [u8; 3],
    header: ClientCommandHeader,
}

#[derive(Serialize)]
struct ClientProcessHeader {
    request_number: u64,
    sector_index: u64
}
