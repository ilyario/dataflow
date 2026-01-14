# GarbageCollectorConfig

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**DefaultCutoffPolicy** | Pointer to **string** | The default cutoff policy. Policies can be one of: - number of commits as an integer value - a duration (see java.time.Duration) - an ISO instant - &#39;NONE&#39;, means everything&#39;s considered as live | [optional] 
**PerRefCutoffPolicies** | Pointer to [**[]ReferenceCutoffPolicy**](ReferenceCutoffPolicy.md) |  | [optional] 
**NewFilesGracePeriod** | Pointer to **string** | Files that have been created after &#39;gc-start-time - new-files-grace-period&#39; are not being deleted. | [optional] 
**ExpectedFileCountPerContent** | Pointer to **int32** |  | [optional] 

## Methods

### NewGarbageCollectorConfig

`func NewGarbageCollectorConfig() *GarbageCollectorConfig`

NewGarbageCollectorConfig instantiates a new GarbageCollectorConfig object
This constructor will assign default values to properties that have it defined,
and makes sure properties required by API are set, but the set of arguments
will change when the set of required properties is changed

### NewGarbageCollectorConfigWithDefaults

`func NewGarbageCollectorConfigWithDefaults() *GarbageCollectorConfig`

NewGarbageCollectorConfigWithDefaults instantiates a new GarbageCollectorConfig object
This constructor will only assign default values to properties that have it defined,
but it doesn't guarantee that properties required by API are set

### GetDefaultCutoffPolicy

`func (o *GarbageCollectorConfig) GetDefaultCutoffPolicy() string`

GetDefaultCutoffPolicy returns the DefaultCutoffPolicy field if non-nil, zero value otherwise.

### GetDefaultCutoffPolicyOk

`func (o *GarbageCollectorConfig) GetDefaultCutoffPolicyOk() (*string, bool)`

GetDefaultCutoffPolicyOk returns a tuple with the DefaultCutoffPolicy field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetDefaultCutoffPolicy

`func (o *GarbageCollectorConfig) SetDefaultCutoffPolicy(v string)`

SetDefaultCutoffPolicy sets DefaultCutoffPolicy field to given value.

### HasDefaultCutoffPolicy

`func (o *GarbageCollectorConfig) HasDefaultCutoffPolicy() bool`

HasDefaultCutoffPolicy returns a boolean if a field has been set.

### GetPerRefCutoffPolicies

`func (o *GarbageCollectorConfig) GetPerRefCutoffPolicies() []ReferenceCutoffPolicy`

GetPerRefCutoffPolicies returns the PerRefCutoffPolicies field if non-nil, zero value otherwise.

### GetPerRefCutoffPoliciesOk

`func (o *GarbageCollectorConfig) GetPerRefCutoffPoliciesOk() (*[]ReferenceCutoffPolicy, bool)`

GetPerRefCutoffPoliciesOk returns a tuple with the PerRefCutoffPolicies field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetPerRefCutoffPolicies

`func (o *GarbageCollectorConfig) SetPerRefCutoffPolicies(v []ReferenceCutoffPolicy)`

SetPerRefCutoffPolicies sets PerRefCutoffPolicies field to given value.

### HasPerRefCutoffPolicies

`func (o *GarbageCollectorConfig) HasPerRefCutoffPolicies() bool`

HasPerRefCutoffPolicies returns a boolean if a field has been set.

### GetNewFilesGracePeriod

`func (o *GarbageCollectorConfig) GetNewFilesGracePeriod() string`

GetNewFilesGracePeriod returns the NewFilesGracePeriod field if non-nil, zero value otherwise.

### GetNewFilesGracePeriodOk

`func (o *GarbageCollectorConfig) GetNewFilesGracePeriodOk() (*string, bool)`

GetNewFilesGracePeriodOk returns a tuple with the NewFilesGracePeriod field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetNewFilesGracePeriod

`func (o *GarbageCollectorConfig) SetNewFilesGracePeriod(v string)`

SetNewFilesGracePeriod sets NewFilesGracePeriod field to given value.

### HasNewFilesGracePeriod

`func (o *GarbageCollectorConfig) HasNewFilesGracePeriod() bool`

HasNewFilesGracePeriod returns a boolean if a field has been set.

### GetExpectedFileCountPerContent

`func (o *GarbageCollectorConfig) GetExpectedFileCountPerContent() int32`

GetExpectedFileCountPerContent returns the ExpectedFileCountPerContent field if non-nil, zero value otherwise.

### GetExpectedFileCountPerContentOk

`func (o *GarbageCollectorConfig) GetExpectedFileCountPerContentOk() (*int32, bool)`

GetExpectedFileCountPerContentOk returns a tuple with the ExpectedFileCountPerContent field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetExpectedFileCountPerContent

`func (o *GarbageCollectorConfig) SetExpectedFileCountPerContent(v int32)`

SetExpectedFileCountPerContent sets ExpectedFileCountPerContent field to given value.

### HasExpectedFileCountPerContent

`func (o *GarbageCollectorConfig) HasExpectedFileCountPerContent() bool`

HasExpectedFileCountPerContent returns a boolean if a field has been set.


[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


