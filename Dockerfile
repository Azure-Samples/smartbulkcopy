FROM mcr.microsoft.com/dotnet/core/sdk:3.1 AS build-env
WORKDIR /app
COPY . .
RUN dotnet publish -c Release -o out

FROM mcr.microsoft.com/dotnet/core/runtime:3.1
WORKDIR /app/client
COPY --from=build-env /app/out .
ENTRYPOINT ["dotnet", "SmartBulkCopy.dll"]
