use anomaly::BoxError;

use ibc::{
    ics02_client::{client_state::ClientState, events::UpdateClient},
    ics03_connection::events::Attributes as ConnectionAttributes,
    ics04_channel::events::{
        Attributes, CloseInit, SendPacket, TimeoutPacket, WriteAcknowledgement,
    },
    ics24_host::identifier::{ChainId, ChannelId, ClientId, ConnectionId, PortId},
    Height,
};

use crate::chain::{
    counterparty::{
        channel_connection_client, counterparty_chain_from_channel,
        counterparty_chain_from_connection,
    },
    handle::ChainHandle,
};

/// Client
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Client {
    /// Destination chain identifier.
    /// This is the chain hosting the client.
    pub dst_chain_id: ChainId,

    /// Client identifier (allocated on the destination chain `dst_chain_id`).
    pub dst_client_id: ClientId,

    /// Source chain identifier.
    /// This is the chain whose headers the client worker is verifying.
    pub src_chain_id: ChainId,
}

impl Client {
    pub fn short_name(&self) -> String {
        format!(
            "client::{}->{}:{}",
            self.src_chain_id, self.dst_chain_id, self.dst_client_id
        )
    }
}

/// Connection
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Connection {
    /// Destination chain identifier.
    pub dst_chain_id: ChainId,

    /// Source chain identifier.
    pub src_chain_id: ChainId,

    /// Source connection identifier.
    pub src_connection_id: ConnectionId,
}

impl Connection {
    pub fn short_name(&self) -> String {
        format!(
            "connection::{}:{} -> {}",
            self.src_connection_id, self.src_chain_id, self.dst_chain_id,
        )
    }
}

/// Channel
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Channel {
    /// Destination chain identifier.
    pub dst_chain_id: ChainId,

    /// Source chain identifier.
    pub src_chain_id: ChainId,

    /// Source channel identifier.
    pub src_channel_id: ChannelId,

    /// Source port identifier.
    pub src_port_id: PortId,
}

impl Channel {
    pub fn short_name(&self) -> String {
        format!(
            "channel::{}/{}:{} -> {}",
            self.src_channel_id, self.src_port_id, self.src_chain_id, self.dst_chain_id,
        )
    }
}

/// A packet worker between a source and destination chain, and a specific channel and port.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Packet {
    /// Destination chain identifier.
    pub dst_chain_id: ChainId,

    /// Source chain identifier.
    pub src_chain_id: ChainId,

    /// Source channel identifier.
    pub src_channel_id: ChannelId,

    /// Source port identifier.
    pub src_port_id: PortId,
}

impl Packet {
    pub fn short_name(&self) -> String {
        format!(
            "packet::{}/{}:{}->{}",
            self.src_channel_id, self.src_port_id, self.src_chain_id, self.dst_chain_id,
        )
    }
}

/// An object determines the amount of parallelism that can
/// be exercised when processing [`IbcEvent`] between
/// two chains. For each [`Object`], a corresponding
/// [`Worker`] is spawned and all [`IbcEvent`]s mapped
/// to an [`Object`] are sent to the associated [`Worker`]
/// for processing.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum Object {
    /// See [`Client`].
    Client(Client),
    /// See [`Connection`].
    Connection(Connection),
    /// See [`Channel`].
    Channel(Channel),
    /// See [`Packet`].
    Packet(Packet),
}

impl Object {
    /// Returns `true` if this [`Object`] is for a [`Worker`] which is interested
    /// in new block events originating from the chain with the given [`ChainId`].
    /// Returns `false` otherwise.
    pub fn notify_new_block(&self, src_chain_id: &ChainId) -> bool {
        match self {
            Object::Client(_) => false,
            Object::Connection(c) => &c.src_chain_id == src_chain_id,
            Object::Channel(c) => &c.src_chain_id == src_chain_id,
            Object::Packet(p) => &p.src_chain_id == src_chain_id,
        }
    }

    /// Returns whether or not this object pertains to the given chain.
    pub fn for_chain(&self, chain_id: &ChainId) -> bool {
        match self {
            Object::Client(c) => &c.src_chain_id == chain_id || &c.dst_chain_id == chain_id,
            Object::Connection(c) => &c.src_chain_id == chain_id || &c.dst_chain_id == chain_id,
            Object::Channel(c) => &c.src_chain_id == chain_id || &c.dst_chain_id == chain_id,
            Object::Packet(p) => &p.src_chain_id == chain_id || &p.dst_chain_id == chain_id,
        }
    }

    /// Return the type of object
    pub fn object_type(&self) -> ObjectType {
        match self {
            Object::Client(_) => ObjectType::Client,
            Object::Channel(_) => ObjectType::Channel,
            Object::Connection(_) => ObjectType::Connection,
            Object::Packet(_) => ObjectType::Packet,
        }
    }
}

#[derive(Copy, Clone, Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub enum ObjectType {
    Client,
    Channel,
    Connection,
    Packet,
}

impl From<Client> for Object {
    fn from(c: Client) -> Self {
        Self::Client(c)
    }
}

impl From<Connection> for Object {
    fn from(c: Connection) -> Self {
        Self::Connection(c)
    }
}

impl From<Channel> for Object {
    fn from(c: Channel) -> Self {
        Self::Channel(c)
    }
}

impl From<Packet> for Object {
    fn from(p: Packet) -> Self {
        Self::Packet(p)
    }
}

impl Object {
    pub fn src_chain_id(&self) -> &ChainId {
        match self {
            Self::Client(ref client) => &client.src_chain_id,
            Self::Connection(ref connection) => &connection.src_chain_id,
            Self::Channel(ref channel) => &channel.src_chain_id,
            Self::Packet(ref path) => &path.src_chain_id,
        }
    }

    pub fn dst_chain_id(&self) -> &ChainId {
        match self {
            Self::Client(ref client) => &client.dst_chain_id,
            Self::Connection(ref connection) => &connection.dst_chain_id,
            Self::Channel(ref channel) => &channel.dst_chain_id,
            Self::Packet(ref path) => &path.dst_chain_id,
        }
    }

    pub fn short_name(&self) -> String {
        match self {
            Self::Client(ref client) => client.short_name(),
            Self::Connection(ref connection) => connection.short_name(),
            Self::Channel(ref channel) => channel.short_name(),
            Self::Packet(ref path) => path.short_name(),
        }
    }

    /// Build the object associated with the given [`UpdateClient`] event.
    pub fn for_update_client(
        e: &UpdateClient,
        dst_chain: &dyn ChainHandle,
    ) -> Result<Self, BoxError> {
        let client_state = dst_chain.query_client_state(e.client_id(), Height::zero())?;
        if client_state.refresh_period().is_none() {
            return Err(format!(
                "client '{}' on chain {} does not require refresh",
                e.client_id(),
                dst_chain.id()
            )
            .into());
        }

        let src_chain_id = client_state.chain_id();

        Ok(Client {
            dst_client_id: e.client_id().clone(),
            dst_chain_id: dst_chain.id(),
            src_chain_id,
        }
        .into())
    }

    /// Build the client object associated with the given channel event attributes.
    pub fn client_from_chan_open_events(
        e: &Attributes,          // The attributes of the emitted event
        chain: &dyn ChainHandle, // The chain which emitted the event
    ) -> Result<Self, BoxError> {
        let channel_id = e
            .channel_id()
            .ok_or_else(|| format!("channel_id missing in channel open event '{:?}'", e))?;

        let client = channel_connection_client(chain, e.port_id(), channel_id)?.client;
        if client.client_state.refresh_period().is_none() {
            return Err(format!(
                "client '{}' on chain {} does not require refresh",
                client.client_id,
                chain.id()
            )
            .into());
        }

        Ok(Client {
            dst_client_id: client.client_id.clone(),
            dst_chain_id: chain.id(), // The object's destination is the chain hosting the client
            src_chain_id: client.client_state.chain_id(),
        }
        .into())
    }

    /// Build the Connection object associated with the given [`Open`] connection event.
    pub fn connection_from_conn_open_events(
        e: &ConnectionAttributes,
        src_chain: &dyn ChainHandle,
    ) -> Result<Self, BoxError> {
        let connection_id = e.connection_id.as_ref().ok_or_else(|| {
            format!(
                "connection_id missing from connection handshake event '{:?}'",
                e
            )
        })?;

        let dst_chain_id =
            counterparty_chain_from_connection(src_chain, &connection_id).map_err(|_| {
                "destination chain id not found during conn open handshake step".to_string()
            })?;

        Ok(Connection {
            dst_chain_id,
            src_chain_id: src_chain.id(),
            src_connection_id: connection_id.clone(),
        }
        .into())
    }

    /// Build the Channel object associated with the given [`Open`] channel event.
    pub fn channel_from_chan_open_events(
        e: &Attributes,
        src_chain: &dyn ChainHandle,
    ) -> Result<Self, BoxError> {
        let channel_id = e
            .channel_id()
            .ok_or_else(|| format!("channel_id missing in OpenInit event '{:?}'", e))?;

        let dst_chain_id = counterparty_chain_from_channel(src_chain, channel_id, &e.port_id())
            .map_err(|_| "dest chain missing in init".to_string())?;

        Ok(Channel {
            dst_chain_id,
            src_chain_id: src_chain.id(),
            src_channel_id: channel_id.clone(),
            src_port_id: e.port_id().clone(),
        }
        .into())
    }

    /// Build the object associated with the given [`SendPacket`] event.
    pub fn for_send_packet(e: &SendPacket, src_chain: &dyn ChainHandle) -> Result<Self, BoxError> {
        let dst_chain_id = counterparty_chain_from_channel(
            src_chain,
            &e.packet.source_channel,
            &e.packet.source_port,
        )?;

        Ok(Packet {
            dst_chain_id,
            src_chain_id: src_chain.id(),
            src_channel_id: e.packet.source_channel.clone(),
            src_port_id: e.packet.source_port.clone(),
        }
        .into())
    }

    /// Build the object associated with the given [`WriteAcknowledgement`] event.
    pub fn for_write_ack(
        e: &WriteAcknowledgement,
        src_chain: &dyn ChainHandle,
    ) -> Result<Self, BoxError> {
        let dst_chain_id = counterparty_chain_from_channel(
            src_chain,
            &e.packet.destination_channel,
            &e.packet.destination_port,
        )?;

        Ok(Packet {
            dst_chain_id,
            src_chain_id: src_chain.id(),
            src_channel_id: e.packet.destination_channel.clone(),
            src_port_id: e.packet.destination_port.clone(),
        }
        .into())
    }

    /// Build the object associated with the given [`TimeoutPacket`] event.
    pub fn for_timeout_packet(
        e: &TimeoutPacket,
        src_chain: &dyn ChainHandle,
    ) -> Result<Self, BoxError> {
        let dst_chain_id = counterparty_chain_from_channel(
            src_chain,
            &e.packet.source_channel,
            &e.packet.source_port,
        )?;

        Ok(Packet {
            dst_chain_id,
            src_chain_id: src_chain.id(),
            src_channel_id: e.src_channel_id().clone(),
            src_port_id: e.src_port_id().clone(),
        }
        .into())
    }

    /// Build the object associated with the given [`CloseInit`] event.
    pub fn for_close_init_channel(
        e: &CloseInit,
        src_chain: &dyn ChainHandle,
    ) -> Result<Self, BoxError> {
        let dst_chain_id =
            counterparty_chain_from_channel(src_chain, e.channel_id(), &e.port_id())?;

        Ok(Packet {
            dst_chain_id,
            src_chain_id: src_chain.id(),
            src_channel_id: e.channel_id().clone(),
            src_port_id: e.port_id().clone(),
        }
        .into())
    }
}
