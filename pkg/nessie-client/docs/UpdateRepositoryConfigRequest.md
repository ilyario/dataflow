# UpdateRepositoryConfigRequest

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**Config** | Pointer to [**GarbageCollectorConfig**](GarbageCollectorConfig.md) |  | [optional] 

## Methods

### NewUpdateRepositoryConfigRequest

`func NewUpdateRepositoryConfigRequest() *UpdateRepositoryConfigRequest`

NewUpdateRepositoryConfigRequest instantiates a new UpdateRepositoryConfigRequest object
This constructor will assign default values to properties that have it defined,
and makes sure properties required by API are set, but the set of arguments
will change when the set of required properties is changed

### NewUpdateRepositoryConfigRequestWithDefaults

`func NewUpdateRepositoryConfigRequestWithDefaults() *UpdateRepositoryConfigRequest`

NewUpdateRepositoryConfigRequestWithDefaults instantiates a new UpdateRepositoryConfigRequest object
This constructor will only assign default values to properties that have it defined,
but it doesn't guarantee that properties required by API are set

### GetConfig

`func (o *UpdateRepositoryConfigRequest) GetConfig() GarbageCollectorConfig`

GetConfig returns the Config field if non-nil, zero value otherwise.

### GetConfigOk

`func (o *UpdateRepositoryConfigRequest) GetConfigOk() (*GarbageCollectorConfig, bool)`

GetConfigOk returns a tuple with the Config field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetConfig

`func (o *UpdateRepositoryConfigRequest) SetConfig(v GarbageCollectorConfig)`

SetConfig sets Config field to given value.

### HasConfig

`func (o *UpdateRepositoryConfigRequest) HasConfig() bool`

HasConfig returns a boolean if a field has been set.


[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


