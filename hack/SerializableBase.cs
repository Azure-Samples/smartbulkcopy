using Microsoft.Data.SqlClient.Server;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Data.SqlTypes;
using System.IO;
using System.Linq;

namespace Microsoft.SqlServer.Types
{
    public class SerializableBase : IBinarySerialize, INullable, IComparable
    {
        private MemoryStream _ms;
        private bool _null;

        public bool IsNull => _null; 

        public SerializableBase()
        {
            this._null = true;
            this._ms = new MemoryStream();
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

            w.Write(_ms.ToArray());
        }

        [SqlMethod(IsDeterministic = true, IsPrecise = true)]
        public void Read(BinaryReader r)
        {
            if (r is null)
                throw new ArgumentException(nameof(r));

            const int bufferSize = 1024;
            byte[] buffer = new byte[bufferSize];
            int count;
            while ((count = r.Read(buffer, 0, buffer.Length)) != 0)
                _ms.Write(buffer, 0, count);            

            this._null = false;
        }
    }
}
