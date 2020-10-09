using Microsoft.Data.SqlClient.Server;

namespace Microsoft.SqlServer.Types
{
    [SqlUserDefinedType(Format.UserDefined, IsByteOrdered = true, MaxByteSize = 892)]
    public class SqlHierarchyId : SerializableBase
    { }
}
