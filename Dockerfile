FROM mcr.microsoft.com/dotnet/core/sdk:3.1 AS build-env
WORKDIR /app

# Copy csproj and restore as distinct layers
COPY client/*.csproj ./client
COPY hack/*.csproj ./hack
RUN dotnet restore

# Copy everything else and build
COPY . ./
RUN dotnet publish -c Release -o out

# Build runtime image
FROM mcr.microsoft.com/dotnet/core/runtime:3.1
WORKDIR /app/client
COPY --from=build-env /app/out .
ENTRYPOINT ["dotnet", "SmartBulkCopy.dll", "config/smartbulkcopy.config"]
