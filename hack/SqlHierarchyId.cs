using Microsoft.Data.SqlClient.Server;
using System;
using System.Data.SqlTypes;

namespace Microsoft.SqlServer.Types
{
    [SqlUserDefinedType(Format.UserDefined, IsByteOrdered = true, MaxByteSize = 892)]
    public class SqlHierarchyId : SerializableBase
    { }
}
