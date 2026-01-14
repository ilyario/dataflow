# NessieConfiguration

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**DefaultBranch** | Pointer to **string** |  | [optional] 
**MinSupportedApiVersion** | Pointer to **int32** |  | [optional] 
**MaxSupportedApiVersion** | Pointer to **int32** |  | [optional] 
**ActualApiVersion** | Pointer to **int32** |  | [optional] 
**SpecVersion** | Pointer to **string** | Semver version representing the behavior of the Nessie server.  Additional functionality might be added to Nessie servers within a \&quot;spec major version\&quot; in a non-breaking way. Clients are encouraged to check the spec version when using such added functionality. | [optional] 
**NoAncestorHash** | Pointer to **string** |  | [optional] 
**RepositoryCreationTimestamp** | Pointer to **time.Time** |  | [optional] 
**OldestPossibleCommitTimestamp** | Pointer to **time.Time** |  | [optional] 
**AdditionalPropertiesField** | Pointer to **map[string]string** |  | [optional] 

## Methods

### NewNessieConfiguration

`func NewNessieConfiguration() *NessieConfiguration`

NewNessieConfiguration instantiates a new NessieConfiguration object
This constructor will assign default values to properties that have it defined,
and makes sure properties required by API are set, but the set of arguments
will change when the set of required properties is changed

### NewNessieConfigurationWithDefaults

`func NewNessieConfigurationWithDefaults() *NessieConfiguration`

NewNessieConfigurationWithDefaults instantiates a new NessieConfiguration object
This constructor will only assign default values to properties that have it defined,
but it doesn't guarantee that properties required by API are set

### GetDefaultBranch

`func (o *NessieConfiguration) GetDefaultBranch() string`

GetDefaultBranch returns the DefaultBranch field if non-nil, zero value otherwise.

### GetDefaultBranchOk

`func (o *NessieConfiguration) GetDefaultBranchOk() (*string, bool)`

GetDefaultBranchOk returns a tuple with the DefaultBranch field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetDefaultBranch

`func (o *NessieConfiguration) SetDefaultBranch(v string)`

SetDefaultBranch sets DefaultBranch field to given value.

### HasDefaultBranch

`func (o *NessieConfiguration) HasDefaultBranch() bool`

HasDefaultBranch returns a boolean if a field has been set.

### GetMinSupportedApiVersion

`func (o *NessieConfiguration) GetMinSupportedApiVersion() int32`

GetMinSupportedApiVersion returns the MinSupportedApiVersion field if non-nil, zero value otherwise.

### GetMinSupportedApiVersionOk

`func (o *NessieConfiguration) GetMinSupportedApiVersionOk() (*int32, bool)`

GetMinSupportedApiVersionOk returns a tuple with the MinSupportedApiVersion field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetMinSupportedApiVersion

`func (o *NessieConfiguration) SetMinSupportedApiVersion(v int32)`

SetMinSupportedApiVersion sets MinSupportedApiVersion field to given value.

### HasMinSupportedApiVersion

`func (o *NessieConfiguration) HasMinSupportedApiVersion() bool`

HasMinSupportedApiVersion returns a boolean if a field has been set.

### GetMaxSupportedApiVersion

`func (o *NessieConfiguration) GetMaxSupportedApiVersion() int32`

GetMaxSupportedApiVersion returns the MaxSupportedApiVersion field if non-nil, zero value otherwise.

### GetMaxSupportedApiVersionOk

`func (o *NessieConfiguration) GetMaxSupportedApiVersionOk() (*int32, bool)`

GetMaxSupportedApiVersionOk returns a tuple with the MaxSupportedApiVersion field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetMaxSupportedApiVersion

`func (o *NessieConfiguration) SetMaxSupportedApiVersion(v int32)`

SetMaxSupportedApiVersion sets MaxSupportedApiVersion field to given value.

### HasMaxSupportedApiVersion

`func (o *NessieConfiguration) HasMaxSupportedApiVersion() bool`

HasMaxSupportedApiVersion returns a boolean if a field has been set.

### GetActualApiVersion

`func (o *NessieConfiguration) GetActualApiVersion() int32`

GetActualApiVersion returns the ActualApiVersion field if non-nil, zero value otherwise.

### GetActualApiVersionOk

`func (o *NessieConfiguration) GetActualApiVersionOk() (*int32, bool)`

GetActualApiVersionOk returns a tuple with the ActualApiVersion field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetActualApiVersion

`func (o *NessieConfiguration) SetActualApiVersion(v int32)`

SetActualApiVersion sets ActualApiVersion field to given value.

### HasActualApiVersion

`func (o *NessieConfiguration) HasActualApiVersion() bool`

HasActualApiVersion returns a boolean if a field has been set.

### GetSpecVersion

`func (o *NessieConfiguration) GetSpecVersion() string`

GetSpecVersion returns the SpecVersion field if non-nil, zero value otherwise.

### GetSpecVersionOk

`func (o *NessieConfiguration) GetSpecVersionOk() (*string, bool)`

GetSpecVersionOk returns a tuple with the SpecVersion field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetSpecVersion

`func (o *NessieConfiguration) SetSpecVersion(v string)`

SetSpecVersion sets SpecVersion field to given value.

### HasSpecVersion

`func (o *NessieConfiguration) HasSpecVersion() bool`

HasSpecVersion returns a boolean if a field has been set.

### GetNoAncestorHash

`func (o *NessieConfiguration) GetNoAncestorHash() string`

GetNoAncestorHash returns the NoAncestorHash field if non-nil, zero value otherwise.

### GetNoAncestorHashOk

`func (o *NessieConfiguration) GetNoAncestorHashOk() (*string, bool)`

GetNoAncestorHashOk returns a tuple with the NoAncestorHash field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetNoAncestorHash

`func (o *NessieConfiguration) SetNoAncestorHash(v string)`

SetNoAncestorHash sets NoAncestorHash field to given value.

### HasNoAncestorHash

`func (o *NessieConfiguration) HasNoAncestorHash() bool`

HasNoAncestorHash returns a boolean if a field has been set.

### GetRepositoryCreationTimestamp

`func (o *NessieConfiguration) GetRepositoryCreationTimestamp() time.Time`

GetRepositoryCreationTimestamp returns the RepositoryCreationTimestamp field if non-nil, zero value otherwise.

### GetRepositoryCreationTimestampOk

`func (o *NessieConfiguration) GetRepositoryCreationTimestampOk() (*time.Time, bool)`

GetRepositoryCreationTimestampOk returns a tuple with the RepositoryCreationTimestamp field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetRepositoryCreationTimestamp

`func (o *NessieConfiguration) SetRepositoryCreationTimestamp(v time.Time)`

SetRepositoryCreationTimestamp sets RepositoryCreationTimestamp field to given value.

### HasRepositoryCreationTimestamp

`func (o *NessieConfiguration) HasRepositoryCreationTimestamp() bool`

HasRepositoryCreationTimestamp returns a boolean if a field has been set.

### GetOldestPossibleCommitTimestamp

`func (o *NessieConfiguration) GetOldestPossibleCommitTimestamp() time.Time`

GetOldestPossibleCommitTimestamp returns the OldestPossibleCommitTimestamp field if non-nil, zero value otherwise.

### GetOldestPossibleCommitTimestampOk

`func (o *NessieConfiguration) GetOldestPossibleCommitTimestampOk() (*time.Time, bool)`

GetOldestPossibleCommitTimestampOk returns a tuple with the OldestPossibleCommitTimestamp field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetOldestPossibleCommitTimestamp

`func (o *NessieConfiguration) SetOldestPossibleCommitTimestamp(v time.Time)`

SetOldestPossibleCommitTimestamp sets OldestPossibleCommitTimestamp field to given value.

### HasOldestPossibleCommitTimestamp

`func (o *NessieConfiguration) HasOldestPossibleCommitTimestamp() bool`

HasOldestPossibleCommitTimestamp returns a boolean if a field has been set.

### GetAdditionalPropertiesField

`func (o *NessieConfiguration) GetAdditionalPropertiesField() map[string]string`

GetAdditionalPropertiesField returns the AdditionalPropertiesField field if non-nil, zero value otherwise.

### GetAdditionalPropertiesFieldOk

`func (o *NessieConfiguration) GetAdditionalPropertiesFieldOk() (*map[string]string, bool)`

GetAdditionalPropertiesFieldOk returns a tuple with the AdditionalPropertiesField field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetAdditionalPropertiesField

`func (o *NessieConfiguration) SetAdditionalPropertiesField(v map[string]string)`

SetAdditionalPropertiesField sets AdditionalPropertiesField field to given value.

### HasAdditionalPropertiesField

`func (o *NessieConfiguration) HasAdditionalPropertiesField() bool`

HasAdditionalPropertiesField returns a boolean if a field has been set.


[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


