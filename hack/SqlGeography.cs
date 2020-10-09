using Microsoft.Data.SqlClient.Server;

namespace Microsoft.SqlServer.Types
{
    [SqlUserDefinedType(Format.UserDefined, IsByteOrdered = false, MaxByteSize = -1, IsFixedLength = false)]
    public class SqlGeography : SerializableBase
    { }
}