# GarbageCollectorConfigV2

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**DefaultCutoffPolicy** | Pointer to **string** | The default cutoff policy. Policies can be one of: - number of commits as an integer value - a duration (see java.time.Duration) - an ISO instant - &#39;NONE&#39;, means everything&#39;s considered as live | [optional] 
**PerRefCutoffPolicies** | Pointer to [**[]ReferenceCutoffPolicyV2**](ReferenceCutoffPolicyV2.md) |  | [optional] 
**NewFilesGracePeriod** | Pointer to **string** | Files that have been created after &#39;gc-start-time - new-files-grace-period&#39; are not being deleted. | [optional] 
**ExpectedFileCountPerContent** | Pointer to **int32** |  | [optional] 

## Methods

### NewGarbageCollectorConfigV2

`func NewGarbageCollectorConfigV2() *GarbageCollectorConfigV2`

NewGarbageCollectorConfigV2 instantiates a new GarbageCollectorConfigV2 object
This constructor will assign default values to properties that have it defined,
and makes sure properties required by API are set, but the set of arguments
will change when the set of required properties is changed

### NewGarbageCollectorConfigV2WithDefaults

`func NewGarbageCollectorConfigV2WithDefaults() *GarbageCollectorConfigV2`

NewGarbageCollectorConfigV2WithDefaults instantiates a new GarbageCollectorConfigV2 object
This constructor will only assign default values to properties that have it defined,
but it doesn't guarantee that properties required by API are set

### GetDefaultCutoffPolicy

`func (o *GarbageCollectorConfigV2) GetDefaultCutoffPolicy() string`

GetDefaultCutoffPolicy returns the DefaultCutoffPolicy field if non-nil, zero value otherwise.

### GetDefaultCutoffPolicyOk

`func (o *GarbageCollectorConfigV2) GetDefaultCutoffPolicyOk() (*string, bool)`

GetDefaultCutoffPolicyOk returns a tuple with the DefaultCutoffPolicy field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetDefaultCutoffPolicy

`func (o *GarbageCollectorConfigV2) SetDefaultCutoffPolicy(v string)`

SetDefaultCutoffPolicy sets DefaultCutoffPolicy field to given value.

### HasDefaultCutoffPolicy

`func (o *GarbageCollectorConfigV2) HasDefaultCutoffPolicy() bool`

HasDefaultCutoffPolicy returns a boolean if a field has been set.

### GetPerRefCutoffPolicies

`func (o *GarbageCollectorConfigV2) GetPerRefCutoffPolicies() []ReferenceCutoffPolicyV2`

GetPerRefCutoffPolicies returns the PerRefCutoffPolicies field if non-nil, zero value otherwise.

### GetPerRefCutoffPoliciesOk

`func (o *GarbageCollectorConfigV2) GetPerRefCutoffPoliciesOk() (*[]ReferenceCutoffPolicyV2, bool)`

GetPerRefCutoffPoliciesOk returns a tuple with the PerRefCutoffPolicies field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetPerRefCutoffPolicies

`func (o *GarbageCollectorConfigV2) SetPerRefCutoffPolicies(v []ReferenceCutoffPolicyV2)`

SetPerRefCutoffPolicies sets PerRefCutoffPolicies field to given value.

### HasPerRefCutoffPolicies

`func (o *GarbageCollectorConfigV2) HasPerRefCutoffPolicies() bool`

HasPerRefCutoffPolicies returns a boolean if a field has been set.

### GetNewFilesGracePeriod

`func (o *GarbageCollectorConfigV2) GetNewFilesGracePeriod() string`

GetNewFilesGracePeriod returns the NewFilesGracePeriod field if non-nil, zero value otherwise.

### GetNewFilesGracePeriodOk

`func (o *GarbageCollectorConfigV2) GetNewFilesGracePeriodOk() (*string, bool)`

GetNewFilesGracePeriodOk returns a tuple with the NewFilesGracePeriod field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetNewFilesGracePeriod

`func (o *GarbageCollectorConfigV2) SetNewFilesGracePeriod(v string)`

SetNewFilesGracePeriod sets NewFilesGracePeriod field to given value.

### HasNewFilesGracePeriod

`func (o *GarbageCollectorConfigV2) HasNewFilesGracePeriod() bool`

HasNewFilesGracePeriod returns a boolean if a field has been set.

### GetExpectedFileCountPerContent

`func (o *GarbageCollectorConfigV2) GetExpectedFileCountPerContent() int32`

GetExpectedFileCountPerContent returns the ExpectedFileCountPerContent field if non-nil, zero value otherwise.

### GetExpectedFileCountPerContentOk

`func (o *GarbageCollectorConfigV2) GetExpectedFileCountPerContentOk() (*int32, bool)`

GetExpectedFileCountPerContentOk returns a tuple with the ExpectedFileCountPerContent field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetExpectedFileCountPerContent

`func (o *GarbageCollectorConfigV2) SetExpectedFileCountPerContent(v int32)`

SetExpectedFileCountPerContent sets ExpectedFileCountPerContent field to given value.

### HasExpectedFileCountPerContent

`func (o *GarbageCollectorConfigV2) HasExpectedFileCountPerContent() bool`

HasExpectedFileCountPerContent returns a boolean if a field has been set.


[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


