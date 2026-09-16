// GridPulse EU — Week 1 infra
// Creates: Function App (Linux, Python, consumption plan), a separate ADLS Gen2
// data lake with bronze/silver/gold/export containers, Application Insights,
// a system-assigned managed identity with Storage Blob Data Contributor on the
// data lake (no connection strings for data access), and a monthly budget alert.
//
// Deploy with:
//   az group create --name rg-gridpulse --location westeurope
//   az deployment group create --resource-group rg-gridpulse \
//     --template-file infra/main.bicep \
//     --parameters alertEmail=you@example.com monthlyBudgetAmount=20 entsoeToken=PASTE_TOKEN_HERE

@description('Short project name used to build resource names (lowercase, no spaces).')
param projectName string = 'gridpulse'

@description('Azure region for all resources.')
param location string = resourceGroup().location

@description('Email address to receive budget alerts.')
param alertEmail string

@description('Monthly budget cap, in your billing currency.')
param monthlyBudgetAmount int = 20

@description('ENTSO-E RESTful API security token. Deploy with a placeholder (e.g. "pending") if you are still waiting on ENTSO-E approval, then redeploy once you have the real one.')
@secure()
param entsoeToken string

@description('Start of the current budget month, computed automatically at deploy time.')
param budgetStartDate string = utcNow('yyyy-MM-01')

var suffix = uniqueString(resourceGroup().id)
// Storage account names must be 3-24 lowercase letters/numbers, so these use a
// short code instead of the full project name (which alone would blow the limit
// once the uniqueness suffix is appended).
var storageShortCode = 'gp'
var funcStorageName = toLower('st${storageShortCode}fn${suffix}')
var dataLakeName = toLower('st${storageShortCode}dl${suffix}')
var appInsightsName = '${projectName}-ai'
var planName = '${projectName}-plan'
var functionAppName = '${projectName}-func-${suffix}'
var storageBlobDataContributorRoleId = 'ba92f5b4-2d11-453d-a403-e96b0029c9fe'

resource funcStorage 'Microsoft.Storage/storageAccounts@2023-01-01' = {
  name: funcStorageName
  location: location
  sku: { name: 'Standard_LRS' }
  kind: 'StorageV2'
  properties: {
    minimumTlsVersion: 'TLS1_2'
    allowBlobPublicAccess: false
  }
}

resource dataLake 'Microsoft.Storage/storageAccounts@2023-01-01' = {
  name: dataLakeName
  location: location
  sku: { name: 'Standard_LRS' }
  kind: 'StorageV2'
  properties: {
    isHnsEnabled: true
    minimumTlsVersion: 'TLS1_2'
    allowBlobPublicAccess: false
  }
}

resource dataLakeBlobService 'Microsoft.Storage/storageAccounts/blobServices@2023-01-01' = {
  parent: dataLake
  name: 'default'
}

resource bronzeContainer 'Microsoft.Storage/storageAccounts/blobServices/containers@2023-01-01' = {
  parent: dataLakeBlobService
  name: 'bronze'
}

resource silverContainer 'Microsoft.Storage/storageAccounts/blobServices/containers@2023-01-01' = {
  parent: dataLakeBlobService
  name: 'silver'
}

resource goldContainer 'Microsoft.Storage/storageAccounts/blobServices/containers@2023-01-01' = {
  parent: dataLakeBlobService
  name: 'gold'
}

resource exportContainer 'Microsoft.Storage/storageAccounts/blobServices/containers@2023-01-01' = {
  parent: dataLakeBlobService
  name: 'export'
}

resource appInsights 'Microsoft.Insights/components@2020-02-02' = {
  name: appInsightsName
  location: location
  kind: 'web'
  properties: {
    Application_Type: 'web'
  }
}

resource plan 'Microsoft.Web/serverfarms@2023-01-01' = {
  name: planName
  location: location
  sku: {
    name: 'Y1'
    tier: 'Dynamic'
  }
  properties: {
    reserved: true
  }
}

resource functionApp 'Microsoft.Web/sites@2023-01-01' = {
  name: functionAppName
  location: location
  kind: 'functionapp,linux'
  identity: {
    type: 'SystemAssigned'
  }
  properties: {
    serverFarmId: plan.id
    httpsOnly: true
    siteConfig: {
      linuxFxVersion: 'PYTHON|3.11'
      appSettings: [
        {
          name: 'AzureWebJobsStorage'
          value: 'DefaultEndpointsProtocol=https;AccountName=${funcStorage.name};AccountKey=${funcStorage.listKeys().keys[0].value};EndpointSuffix=core.windows.net'
        }
        { name: 'FUNCTIONS_WORKER_RUNTIME', value: 'python' }
        { name: 'FUNCTIONS_EXTENSION_VERSION', value: '~4' }
        { name: 'APPINSIGHTS_INSTRUMENTATIONKEY', value: appInsights.properties.InstrumentationKey }
        { name: 'ENTSOE_TOKEN', value: entsoeToken }
        { name: 'ADLS_ACCOUNT_URL', value: 'https://${dataLake.name}.dfs.core.windows.net' }
      ]
    }
  }
}

resource roleAssignment 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(dataLake.id, functionApp.id, storageBlobDataContributorRoleId)
  scope: dataLake
  properties: {
    principalId: functionApp.identity.principalId
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', storageBlobDataContributorRoleId)
    principalType: 'ServicePrincipal'
  }
}

resource budget 'Microsoft.Consumption/budgets@2023-05-01' = {
  name: '${projectName}-monthly-budget'
  properties: {
    category: 'Cost'
    amount: monthlyBudgetAmount
    timeGrain: 'Monthly'
    timePeriod: {
      startDate: budgetStartDate
    }
    notifications: {
      actual_80: {
        enabled: true
        operator: 'GreaterThan'
        threshold: 80
        contactEmails: [ alertEmail ]
      }
      actual_100: {
        enabled: true
        operator: 'GreaterThan'
        threshold: 100
        contactEmails: [ alertEmail ]
      }
    }
  }
}

output functionAppName string = functionApp.name
output dataLakeAccountUrl string = 'https://${dataLake.name}.dfs.core.windows.net'
output resourceGroupName string = resourceGroup().name
