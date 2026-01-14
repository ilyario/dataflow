# UpdateRepositoryConfigResponse

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**Previous** | Pointer to [**GarbageCollectorConfig**](GarbageCollectorConfig.md) |  | [optional] 

## Methods

### NewUpdateRepositoryConfigResponse

`func NewUpdateRepositoryConfigResponse() *UpdateRepositoryConfigResponse`

NewUpdateRepositoryConfigResponse instantiates a new UpdateRepositoryConfigResponse object
This constructor will assign default values to properties that have it defined,
and makes sure properties required by API are set, but the set of arguments
will change when the set of required properties is changed

### NewUpdateRepositoryConfigResponseWithDefaults

`func NewUpdateRepositoryConfigResponseWithDefaults() *UpdateRepositoryConfigResponse`

NewUpdateRepositoryConfigResponseWithDefaults instantiates a new UpdateRepositoryConfigResponse object
This constructor will only assign default values to properties that have it defined,
but it doesn't guarantee that properties required by API are set

### GetPrevious

`func (o *UpdateRepositoryConfigResponse) GetPrevious() GarbageCollectorConfig`

GetPrevious returns the Previous field if non-nil, zero value otherwise.

### GetPreviousOk

`func (o *UpdateRepositoryConfigResponse) GetPreviousOk() (*GarbageCollectorConfig, bool)`

GetPreviousOk returns a tuple with the Previous field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetPrevious

`func (o *UpdateRepositoryConfigResponse) SetPrevious(v GarbageCollectorConfig)`

SetPrevious sets Previous field to given value.

### HasPrevious

`func (o *UpdateRepositoryConfigResponse) HasPrevious() bool`

HasPrevious returns a boolean if a field has been set.


[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


