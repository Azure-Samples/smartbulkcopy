using Microsoft.Data.SqlClient.Server;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Data.SqlTypes;
using System.IO;
using System.Linq;
using System.Text;


namespace Microsoft.SqlServer.Types
{
    [SqlUserDefinedType(Format.UserDefined, IsByteOrdered = true, MaxByteSize = 892, Name = "SqlHierarchyId")]
    public struct SqlHierarchyId : IBinarySerialize, INullable, IComparable
    {
        private byte[] _raw;
        private bool _null;

        public bool IsNull => _null; 

        private SqlHierarchyId(bool isNull = false)
        {
            _null = isNull;
            this._raw = new byte[892];
        }

        public int CompareTo(object obj) => this.CompareTo((SqlHierarchyId)obj);

        public int CompareTo(SqlHierarchyId hid)
        {
            if (IsNull)
            {
                if (!hid.IsNull)
                    return -1;
                return 0;
            }
            if (hid.IsNull)
                return 1;

            return 0;
        }

        [SqlMethod(IsDeterministic = true, IsPrecise = true)]
        public void Write(BinaryWriter w)
        {
            if (w is null)
                throw new ArgumentException(nameof(w));

            w.Write(_raw);
        }

        [SqlMethod(IsDeterministic = true, IsPrecise = true)]
        public void Read(BinaryReader r)
        {
            if (r is null)
                throw new ArgumentException(nameof(r));

            const int bufferSize = 892;
            using (var ms = new MemoryStream())
            {
                byte[] buffer = new byte[bufferSize];
                int count;
                while ((count = r.Read(buffer, 0, buffer.Length)) != 0)
                    ms.Write(buffer, 0, count);
                _raw = ms.ToArray();
            }

            this._null = false;
        }
    }
}
