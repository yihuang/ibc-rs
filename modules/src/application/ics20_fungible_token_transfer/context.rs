use crate::ics04_channel::context::{ChannelKeeper, ChannelReader};
use crate::ics24_host::identifier::HostChain;

/// Captures all the dependencies which the ICS20 module requires to be able to dispatch and
/// process IBC messages.
pub trait Ics20Context<Chain: HostChain>:
    ChannelReader<Chain> + ChannelKeeper<Chain> + Clone
{
}
