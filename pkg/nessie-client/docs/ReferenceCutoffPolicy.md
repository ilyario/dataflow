# ReferenceCutoffPolicy

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**ReferenceNamePattern** | Pointer to **string** | Reference name patterns as a regular expressions. | [optional] 
**Policy** | Pointer to **string** | Policies can be one of: - number of commits as an integer value - a duration (see java.time.Duration) - an ISO instant - &#39;NONE&#39;, means everything&#39;s considered as live | [optional] 

## Methods

### NewReferenceCutoffPolicy

`func NewReferenceCutoffPolicy() *ReferenceCutoffPolicy`

NewReferenceCutoffPolicy instantiates a new ReferenceCutoffPolicy object
This constructor will assign default values to properties that have it defined,
and makes sure properties required by API are set, but the set of arguments
will change when the set of required properties is changed

### NewReferenceCutoffPolicyWithDefaults

`func NewReferenceCutoffPolicyWithDefaults() *ReferenceCutoffPolicy`

NewReferenceCutoffPolicyWithDefaults instantiates a new ReferenceCutoffPolicy object
This constructor will only assign default values to properties that have it defined,
but it doesn't guarantee that properties required by API are set

### GetReferenceNamePattern

`func (o *ReferenceCutoffPolicy) GetReferenceNamePattern() string`

GetReferenceNamePattern returns the ReferenceNamePattern field if non-nil, zero value otherwise.

### GetReferenceNamePatternOk

`func (o *ReferenceCutoffPolicy) GetReferenceNamePatternOk() (*string, bool)`

GetReferenceNamePatternOk returns a tuple with the ReferenceNamePattern field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetReferenceNamePattern

`func (o *ReferenceCutoffPolicy) SetReferenceNamePattern(v string)`

SetReferenceNamePattern sets ReferenceNamePattern field to given value.

### HasReferenceNamePattern

`func (o *ReferenceCutoffPolicy) HasReferenceNamePattern() bool`

HasReferenceNamePattern returns a boolean if a field has been set.

### GetPolicy

`func (o *ReferenceCutoffPolicy) GetPolicy() string`

GetPolicy returns the Policy field if non-nil, zero value otherwise.

### GetPolicyOk

`func (o *ReferenceCutoffPolicy) GetPolicyOk() (*string, bool)`

GetPolicyOk returns a tuple with the Policy field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetPolicy

`func (o *ReferenceCutoffPolicy) SetPolicy(v string)`

SetPolicy sets Policy field to given value.

### HasPolicy

`func (o *ReferenceCutoffPolicy) HasPolicy() bool`

HasPolicy returns a boolean if a field has been set.


[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


