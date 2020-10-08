using Microsoft.Data.SqlClient.Server;
using System;
using System.IO;
using System.Data.SqlTypes;

namespace Microsoft.SqlServer.Types
{
    [SqlUserDefinedType(Format.UserDefined, IsByteOrdered = true, MaxByteSize = 892, Name = "SqlHierarchyId")]
    public struct SqlHierarchyId : IBinarySerialize, INullable, IComparable
    {
        public bool IsNull => throw new NotImplementedException();

        public int CompareTo(object obj)
        {
            throw new NotImplementedException();
        }

        public void Read(BinaryReader r)
        {
            
        }

        public void Write(BinaryWriter w)
        {
            
        }
    }
}