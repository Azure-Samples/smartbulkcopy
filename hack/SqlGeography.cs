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
    [SqlUserDefinedType(Format.UserDefined, IsByteOrdered = false, MaxByteSize = -1, IsFixedLength = false)]
    public struct SqlGeometry : IBinarySerialize, INullable
    {
        private byte[] _raw;
        private bool _null;

        public bool IsNull => _null; 

        private SqlGeometry(bool isNull = false)
        {
            _null = isNull;
            this._raw = new byte[892];
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

            const int bufferSize = 1024;
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
