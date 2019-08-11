FROM microsoft/dotnet:2.1-runtime AS base
WORKDIR /app

FROM microsoft/dotnet:2.1-sdk AS build
WORKDIR /src
COPY hsbulk.csproj ./
RUN dotnet restore hsbulk.csproj
COPY . .
WORKDIR /src
RUN dotnet build hsbulk.csproj -c Release -o /app

FROM build AS publish
RUN dotnet publish hsbulk.csproj -c Release -o /app

FROM base AS final
WORKDIR /app
COPY --from=publish /app .
ENTRYPOINT ["dotnet", "hsbulk.dll"]
