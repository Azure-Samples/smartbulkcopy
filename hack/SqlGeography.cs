using Microsoft.Data.SqlClient.Server;
using System;
using System.Data.SqlTypes;

namespace Microsoft.SqlServer.Types
{
    [SqlUserDefinedType(Format.UserDefined, IsByteOrdered = false, MaxByteSize = -1, IsFixedLength = false)]
    public class SqlGeography : SerializableBase
    { }
}