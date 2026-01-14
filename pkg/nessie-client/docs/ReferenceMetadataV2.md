# ReferenceMetadataV2

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**NumCommitsAhead** | Pointer to **int32** |  | [optional] 
**NumCommitsBehind** | Pointer to **int32** |  | [optional] 
**CommitMetaOfHEAD** | Pointer to [**CommitMetaV2**](CommitMetaV2.md) |  | [optional] 
**CommonAncestorHash** | Pointer to **string** |  | [optional] 
**NumTotalCommits** | Pointer to **int64** |  | [optional] 

## Methods

### NewReferenceMetadataV2

`func NewReferenceMetadataV2() *ReferenceMetadataV2`

NewReferenceMetadataV2 instantiates a new ReferenceMetadataV2 object
This constructor will assign default values to properties that have it defined,
and makes sure properties required by API are set, but the set of arguments
will change when the set of required properties is changed

### NewReferenceMetadataV2WithDefaults

`func NewReferenceMetadataV2WithDefaults() *ReferenceMetadataV2`

NewReferenceMetadataV2WithDefaults instantiates a new ReferenceMetadataV2 object
This constructor will only assign default values to properties that have it defined,
but it doesn't guarantee that properties required by API are set

### GetNumCommitsAhead

`func (o *ReferenceMetadataV2) GetNumCommitsAhead() int32`

GetNumCommitsAhead returns the NumCommitsAhead field if non-nil, zero value otherwise.

### GetNumCommitsAheadOk

`func (o *ReferenceMetadataV2) GetNumCommitsAheadOk() (*int32, bool)`

GetNumCommitsAheadOk returns a tuple with the NumCommitsAhead field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetNumCommitsAhead

`func (o *ReferenceMetadataV2) SetNumCommitsAhead(v int32)`

SetNumCommitsAhead sets NumCommitsAhead field to given value.

### HasNumCommitsAhead

`func (o *ReferenceMetadataV2) HasNumCommitsAhead() bool`

HasNumCommitsAhead returns a boolean if a field has been set.

### GetNumCommitsBehind

`func (o *ReferenceMetadataV2) GetNumCommitsBehind() int32`

GetNumCommitsBehind returns the NumCommitsBehind field if non-nil, zero value otherwise.

### GetNumCommitsBehindOk

`func (o *ReferenceMetadataV2) GetNumCommitsBehindOk() (*int32, bool)`

GetNumCommitsBehindOk returns a tuple with the NumCommitsBehind field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetNumCommitsBehind

`func (o *ReferenceMetadataV2) SetNumCommitsBehind(v int32)`

SetNumCommitsBehind sets NumCommitsBehind field to given value.

### HasNumCommitsBehind

`func (o *ReferenceMetadataV2) HasNumCommitsBehind() bool`

HasNumCommitsBehind returns a boolean if a field has been set.

### GetCommitMetaOfHEAD

`func (o *ReferenceMetadataV2) GetCommitMetaOfHEAD() CommitMetaV2`

GetCommitMetaOfHEAD returns the CommitMetaOfHEAD field if non-nil, zero value otherwise.

### GetCommitMetaOfHEADOk

`func (o *ReferenceMetadataV2) GetCommitMetaOfHEADOk() (*CommitMetaV2, bool)`

GetCommitMetaOfHEADOk returns a tuple with the CommitMetaOfHEAD field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetCommitMetaOfHEAD

`func (o *ReferenceMetadataV2) SetCommitMetaOfHEAD(v CommitMetaV2)`

SetCommitMetaOfHEAD sets CommitMetaOfHEAD field to given value.

### HasCommitMetaOfHEAD

`func (o *ReferenceMetadataV2) HasCommitMetaOfHEAD() bool`

HasCommitMetaOfHEAD returns a boolean if a field has been set.

### GetCommonAncestorHash

`func (o *ReferenceMetadataV2) GetCommonAncestorHash() string`

GetCommonAncestorHash returns the CommonAncestorHash field if non-nil, zero value otherwise.

### GetCommonAncestorHashOk

`func (o *ReferenceMetadataV2) GetCommonAncestorHashOk() (*string, bool)`

GetCommonAncestorHashOk returns a tuple with the CommonAncestorHash field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetCommonAncestorHash

`func (o *ReferenceMetadataV2) SetCommonAncestorHash(v string)`

SetCommonAncestorHash sets CommonAncestorHash field to given value.

### HasCommonAncestorHash

`func (o *ReferenceMetadataV2) HasCommonAncestorHash() bool`

HasCommonAncestorHash returns a boolean if a field has been set.

### GetNumTotalCommits

`func (o *ReferenceMetadataV2) GetNumTotalCommits() int64`

GetNumTotalCommits returns the NumTotalCommits field if non-nil, zero value otherwise.

### GetNumTotalCommitsOk

`func (o *ReferenceMetadataV2) GetNumTotalCommitsOk() (*int64, bool)`

GetNumTotalCommitsOk returns a tuple with the NumTotalCommits field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetNumTotalCommits

`func (o *ReferenceMetadataV2) SetNumTotalCommits(v int64)`

SetNumTotalCommits sets NumTotalCommits field to given value.

### HasNumTotalCommits

`func (o *ReferenceMetadataV2) HasNumTotalCommits() bool`

HasNumTotalCommits returns a boolean if a field has been set.


[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


