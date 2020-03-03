using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Claims;
using System.Threading.Tasks;
using EventStore.Core.Authorization;
using EventStore.Core.Bus;
using EventStore.Core.Cluster.Settings;
using EventStore.Core.Messages;
using EventStore.Core.Services.Storage.ReaderIndex;
using EventStore.Core.Services.Transport.Grpc;
using EventStore.Core.Services.Transport.Http;
using EventStore.Core.Services.Transport.Http.Authentication;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.DependencyInjection;
using HttpStatusCode = EventStore.Transport.Http.HttpStatusCode;
using MidFunc = System.Func<
	Microsoft.AspNetCore.Http.HttpContext,
	System.Func<System.Threading.Tasks.Task>,
	System.Threading.Tasks.Task
>;
using ElectionsService = EventStore.Core.Services.Transport.Grpc.Elections;
using Operations = EventStore.Core.Services.Transport.Grpc.Operations;

namespace EventStore.Core {
	public class ClusterVNodeStartup : IStartup, IHandle<SystemMessage.SystemReady>,
		IHandle<SystemMessage.BecomeShuttingDown> {
		private static readonly PathString PersistentSegment =
			"/event_store.client.persistent_subscriptions.PersistentSubscriptions";

		private static readonly PathString StreamsSegment = "/event_store.client.streams.Streams";
		private static readonly PathString UsersSegment = "/event_store.client.users.Users";
		private static readonly PathString OperationsSegment = "/event_store.client.operations.Operations";
		private static readonly PathString GossipSegment = "/event_store.cluster.Gossip";
		private static readonly PathString ElectionsSegment = "/event_store.cluster.Elections";

		private readonly ISubsystem[] _subsystems;
		private readonly IPublisher _mainQueue;
		private readonly ISubscriber _mainBus;
		private readonly IReadOnlyList<IHttpAuthenticationProvider> _httpAuthenticationProviders;
		private readonly IReadIndex _readIndex;
		private readonly ClusterVNodeSettings _vNodeSettings;
		private readonly KestrelHttpService _externalHttpService;
		private readonly StatusCheck _statusCheck;

		private bool _ready;
		private readonly IAuthorizationProvider _authorizationProvider;
		private readonly MultiQueuedHandler _httpMessageHandler;

		public ClusterVNodeStartup(
			ISubsystem[] subsystems,
			IPublisher mainQueue,
			ISubscriber mainBus,
			MultiQueuedHandler httpMessageHandler,
			IReadOnlyList<IHttpAuthenticationProvider> httpAuthenticationProviders,
			IAuthorizationProvider authorizationProvider,
			IReadIndex readIndex,
			ClusterVNodeSettings vNodeSettings,
			KestrelHttpService externalHttpService) {
			if (subsystems == null) {
				throw new ArgumentNullException(nameof(subsystems));
			}

			if (mainQueue == null) {
				throw new ArgumentNullException(nameof(mainQueue));
			}

			if (httpAuthenticationProviders == null) {
				throw new ArgumentNullException(nameof(httpAuthenticationProviders));
			}

			if(authorizationProvider == null)
				throw new ArgumentNullException(nameof(authorizationProvider));

			if (readIndex == null) {
				throw new ArgumentNullException(nameof(readIndex));
			}

			if (vNodeSettings == null) {
				throw new ArgumentNullException(nameof(vNodeSettings));
			}

			if (externalHttpService == null) {
				throw new ArgumentNullException(nameof(externalHttpService));
			}

			if (mainBus == null) {
				throw new ArgumentNullException(nameof(mainBus));
			}
			_subsystems = subsystems;
			_mainQueue = mainQueue;
			_mainBus = mainBus;
			_httpMessageHandler = httpMessageHandler;
			_httpAuthenticationProviders = httpAuthenticationProviders;
			_authorizationProvider = authorizationProvider;
			_readIndex = readIndex;
			_vNodeSettings = vNodeSettings;
			_externalHttpService = externalHttpService;

			_statusCheck = new StatusCheck(this);
		}

		public void Configure(IApplicationBuilder app) {
			app.Map("/health", _statusCheck.Configure)
				.UseMiddleware<AuthenticationMiddleware>()
				.UseRouting()
				.UseMiddleware<KestrelToInternalBridgeMiddleware>();
			_subsystems
				.Aggregate(app
						.UseWhen(context => context.Request.Path.StartsWithSegments(PersistentSegment),
							inner => inner.UseRouting().UseEndpoints(endpoint =>
								endpoint.MapGrpcService<PersistentSubscriptions>()))
						.UseWhen(context => context.Request.Path.StartsWithSegments(UsersSegment),
							inner => inner.UseRouting().Use(RequireAuthenticated).UseEndpoints(endpoint =>
								endpoint.MapGrpcService<Users>()))
						.UseWhen(context => context.Request.Path.StartsWithSegments(StreamsSegment),
							inner => inner.UseRouting().UseEndpoints(endpoint =>
								endpoint.MapGrpcService<Streams>()))
						.UseWhen(context => context.Request.Path.StartsWithSegments(GossipSegment),
							inner => inner.UseRouting().UseEndpoints(endpoint =>
								endpoint.MapGrpcService<Gossip>()))
						.UseWhen(context => context.Request.Path.StartsWithSegments(ElectionsSegment),
							inner => inner.UseRouting().UseEndpoints(endpoint =>
								endpoint.MapGrpcService<Elections>()))
						.UseWhen(context => context.Request.Path.StartsWithSegments(OperationsSegment),  // TODO JPB figure out how to delete this sadness
							inner => inner.UseRouting().UseEndpoints(endpoint =>
								endpoint.MapGrpcService<Operations>())),
					(b, subsystem) => subsystem.Configure(b));
			var internalDispatcher = new InternalDispatcherEndpoint(_mainQueue, _httpMessageHandler);
			_mainBus.Subscribe(internalDispatcher);
			app.UseMiddleware<AuthorizationMiddleware>()
				.UseLegacyHttp(internalDispatcher.InvokeAsync, _externalHttpService);
		}

		IServiceProvider IStartup.ConfigureServices(IServiceCollection services) => ConfigureServices(services)
			.BuildServiceProvider();

		public IServiceCollection ConfigureServices(IServiceCollection services) {

			var bridge = new KestrelToInternalBridgeMiddleware(_externalHttpService.UriRouter, _externalHttpService.LogHttpRequests, _externalHttpService.AdvertiseAsAddress, _externalHttpService.AdvertiseAsPort);
			return _subsystems
				.Aggregate(services
						.AddRouting()
						.AddSingleton(_httpAuthenticationProviders)
						.AddSingleton(_authorizationProvider)
						.AddSingleton<AuthenticationMiddleware>()
						.AddSingleton<AuthorizationMiddleware>()
						.AddSingleton<KestrelToInternalBridgeMiddleware>(bridge)
						.AddSingleton(_readIndex)
						.AddSingleton(new Streams(_mainQueue, _readIndex,
							_vNodeSettings.MaxAppendSize, _authorizationProvider))
						.AddSingleton(new PersistentSubscriptions(_mainQueue))
						.AddSingleton(new Users(_mainQueue))
						.AddSingleton(new Operations(_mainQueue))
						.AddSingleton(new Gossip(_mainQueue))
						.AddSingleton(new Elections(_mainQueue))
						.AddGrpc().Services,
					(s, subsystem) => subsystem.ConfigureServices(s));
		}

		private static RequestDelegate RequireAuthenticated(RequestDelegate next) =>
			context => {
				if (!context.User.HasClaim(x => x.Type == ClaimTypes.Anonymous)) {
					return next(context);
				}
				context.Response.StatusCode = HttpStatusCode.Unauthorized;
				return Task.CompletedTask;
			};

		public void Handle(SystemMessage.SystemReady message) => _ready = true;

		public void Handle(SystemMessage.BecomeShuttingDown message) => _ready = false;

		private class StatusCheck {
			private readonly ClusterVNodeStartup _startup;

			public StatusCheck(ClusterVNodeStartup startup) {
				if (startup == null) {
					throw new ArgumentNullException(nameof(startup));
				}

				_startup = startup;
			}

			public void Configure(IApplicationBuilder builder) =>
				builder.Use(GetAndHeadOnly)
					.UseRouter(router => router
						.MapMiddlewareGet("live", inner => inner.Use(Live)));

			private MidFunc Live => (context, next) => {
				context.Response.StatusCode = _startup._ready ? 204 : 503;
				return Task.CompletedTask;
			};

			private static MidFunc GetAndHeadOnly => (context, next) => {
				switch (context.Request.Method) {
					case "HEAD":
						context.Request.Method = "GET";
						return next();
					case "GET":
						return next();
					default:
						context.Response.StatusCode = 405;
						return Task.CompletedTask;
				}
			};
		}
	}
}
