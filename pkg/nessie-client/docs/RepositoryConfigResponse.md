# RepositoryConfigResponse

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**Configs** | Pointer to [**[]GarbageCollectorConfig**](GarbageCollectorConfig.md) | The existing configuration objects for the requested types will be returned. Non-existing config objects will not be returned. | [optional] 

## Methods

### NewRepositoryConfigResponse

`func NewRepositoryConfigResponse() *RepositoryConfigResponse`

NewRepositoryConfigResponse instantiates a new RepositoryConfigResponse object
This constructor will assign default values to properties that have it defined,
and makes sure properties required by API are set, but the set of arguments
will change when the set of required properties is changed

### NewRepositoryConfigResponseWithDefaults

`func NewRepositoryConfigResponseWithDefaults() *RepositoryConfigResponse`

NewRepositoryConfigResponseWithDefaults instantiates a new RepositoryConfigResponse object
This constructor will only assign default values to properties that have it defined,
but it doesn't guarantee that properties required by API are set

### GetConfigs

`func (o *RepositoryConfigResponse) GetConfigs() []GarbageCollectorConfig`

GetConfigs returns the Configs field if non-nil, zero value otherwise.

### GetConfigsOk

`func (o *RepositoryConfigResponse) GetConfigsOk() (*[]GarbageCollectorConfig, bool)`

GetConfigsOk returns a tuple with the Configs field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetConfigs

`func (o *RepositoryConfigResponse) SetConfigs(v []GarbageCollectorConfig)`

SetConfigs sets Configs field to given value.

### HasConfigs

`func (o *RepositoryConfigResponse) HasConfigs() bool`

HasConfigs returns a boolean if a field has been set.


[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


