using System;
using System.Net;
using EventStore.ClientAPI;
using EventStore.Core.Services.TimerService;
using EventStore.Core.Tests.ClientAPI.Helpers;
using EventStore.Core.Tests.Helpers;
using Xunit;

namespace EventStore.Core.Tests.ClientAPI
{
    public abstract class SpecificationWithMiniNode : IClassFixture<SpecificationWithMiniNode.SpecificationFixture>
    {
        protected MiniNode _node {get { return _fixture.Node; }}
        protected IEventStoreConnection _conn {get{return _fixture.Connection;}}
        protected IPEndPoint _HttpEndPoint{get { return _fixture.Node.ExtHttpEndPoint; }}
        private SpecificationFixture _fixture;
        protected SpecificationFixture Fixture{get{return _fixture;}}
        protected virtual void Given()
        {
        }

        protected abstract void When();

        protected virtual IEventStoreConnection BuildConnection(MiniNode node)
        {
            return TestConnection.Create(node.TcpEndPoint);
        }

        public sealed class SpecificationFixture : MiniNodeFixture
        {
            private IEventStoreConnection _conn;
            public IEventStoreConnection Connection{get { return _conn; }}
            private Action<Func<MiniNode, IEventStoreConnection>, Action, Action> _initialize;
            private Action<SpecificationWithMiniNode> _assignStashedValues;
            private Exception _setupException;

            public SpecificationFixture()
            {
                _assignStashedValues = _ => { };
                _initialize = (buildConnection, given, @when) =>
                {
                    _initialize = (_, __, ___) => { if(_setupException != null) {throw new ApplicationException("Fixture Initialization failed", _setupException);}};
                    try
                    {
                        _conn = buildConnection(Node);
                        _conn.ConnectAsync().Wait();
                        given();
                        when();
                    }
                    catch (Exception ex)
                    {
                        _setupException = ex;
                        throw new ApplicationException("Fixture Initialization failed", ex);
                    }
                };
            }

            public override void Dispose()
            {
                if(_conn != null)
                    _conn.Close();
                base.Dispose();
            }

            public void EnsureInitialized(Func<MiniNode, IEventStoreConnection> buildConnection, Action given, Action @when)
            {
                _initialize(buildConnection, given, @when);
            }

            //To help migrate from nunit this allows fixture initialized fields to be appended into test class
            public void AddStashedValueAssignment<T>(T ignored, Action<T> assignStashedValues) where T : SpecificationWithMiniNode
            {
                _assignStashedValues += instance => assignStashedValues((T)instance);
            }

            public void AssignStashedValues(SpecificationWithMiniNode scenario)
            {
                _assignStashedValues(scenario);
            }
        }

        public void SetFixture(SpecificationFixture fixture)
        {
            _fixture = fixture;
            _fixture.EnsureInitialized(BuildConnection, Given, When);
            fixture.AssignStashedValues(this);
        }
    }
}