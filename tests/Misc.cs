using NUnit.Framework;
using NLog;
using SmartBulkCopy;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using System;
using DotNetEnv;

namespace SmartBulkCopy.Tests
{
    public class MiscTests: BaseTests
    {                
        [Test]
        public async Task Table_With_ForeignKeys()
        {
            var tar = await AnalyzeTable("schema1.table_with_fk");

            Assert.AreEqual(AnalysisOutcome.ForeignKeysFoundOnDestination, tar.Outcome);
        }

        [Test]
        public async Task Table_With_SecondaryIndexes()
        {
            var tar = await AnalyzeTable("schema1.mix");

            Assert.AreEqual(AnalysisOutcome.SecondaryIndexFoundOnDestination, tar.Outcome);
        }

        [Test]
        public async Task Table_With_SystemVersioning()
        {
            var tar = await AnalyzeTable("schema1.temporal");

            Assert.AreEqual(AnalysisOutcome.DestinationIsTemporalTable, tar.Outcome);
        }    

        [Test]
        public async Task Table_With_HiearchyId()
        {
            var tar = await AnalyzeTable("schema1.hierarchical_data");

            Assert.AreEqual(AnalysisOutcome.Success, tar.Outcome);
        }    

        [Test]
        public async Task Table_With_Geospatial()
        {
            var tar = await AnalyzeTable("schema1.spatial_data");

            Assert.AreEqual(AnalysisOutcome.Success, tar.Outcome);
        }    
    }
}