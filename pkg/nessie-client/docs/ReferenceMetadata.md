# ReferenceMetadata

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**NumCommitsAhead** | Pointer to **int32** |  | [optional] 
**NumCommitsBehind** | Pointer to **int32** |  | [optional] 
**CommitMetaOfHEAD** | Pointer to [**CommitMeta**](CommitMeta.md) |  | [optional] 
**CommonAncestorHash** | Pointer to **string** |  | [optional] 
**NumTotalCommits** | Pointer to **int64** |  | [optional] 

## Methods

### NewReferenceMetadata

`func NewReferenceMetadata() *ReferenceMetadata`

NewReferenceMetadata instantiates a new ReferenceMetadata object
This constructor will assign default values to properties that have it defined,
and makes sure properties required by API are set, but the set of arguments
will change when the set of required properties is changed

### NewReferenceMetadataWithDefaults

`func NewReferenceMetadataWithDefaults() *ReferenceMetadata`

NewReferenceMetadataWithDefaults instantiates a new ReferenceMetadata object
This constructor will only assign default values to properties that have it defined,
but it doesn't guarantee that properties required by API are set

### GetNumCommitsAhead

`func (o *ReferenceMetadata) GetNumCommitsAhead() int32`

GetNumCommitsAhead returns the NumCommitsAhead field if non-nil, zero value otherwise.

### GetNumCommitsAheadOk

`func (o *ReferenceMetadata) GetNumCommitsAheadOk() (*int32, bool)`

GetNumCommitsAheadOk returns a tuple with the NumCommitsAhead field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetNumCommitsAhead

`func (o *ReferenceMetadata) SetNumCommitsAhead(v int32)`

SetNumCommitsAhead sets NumCommitsAhead field to given value.

### HasNumCommitsAhead

`func (o *ReferenceMetadata) HasNumCommitsAhead() bool`

HasNumCommitsAhead returns a boolean if a field has been set.

### GetNumCommitsBehind

`func (o *ReferenceMetadata) GetNumCommitsBehind() int32`

GetNumCommitsBehind returns the NumCommitsBehind field if non-nil, zero value otherwise.

### GetNumCommitsBehindOk

`func (o *ReferenceMetadata) GetNumCommitsBehindOk() (*int32, bool)`

GetNumCommitsBehindOk returns a tuple with the NumCommitsBehind field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetNumCommitsBehind

`func (o *ReferenceMetadata) SetNumCommitsBehind(v int32)`

SetNumCommitsBehind sets NumCommitsBehind field to given value.

### HasNumCommitsBehind

`func (o *ReferenceMetadata) HasNumCommitsBehind() bool`

HasNumCommitsBehind returns a boolean if a field has been set.

### GetCommitMetaOfHEAD

`func (o *ReferenceMetadata) GetCommitMetaOfHEAD() CommitMeta`

GetCommitMetaOfHEAD returns the CommitMetaOfHEAD field if non-nil, zero value otherwise.

### GetCommitMetaOfHEADOk

`func (o *ReferenceMetadata) GetCommitMetaOfHEADOk() (*CommitMeta, bool)`

GetCommitMetaOfHEADOk returns a tuple with the CommitMetaOfHEAD field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetCommitMetaOfHEAD

`func (o *ReferenceMetadata) SetCommitMetaOfHEAD(v CommitMeta)`

SetCommitMetaOfHEAD sets CommitMetaOfHEAD field to given value.

### HasCommitMetaOfHEAD

`func (o *ReferenceMetadata) HasCommitMetaOfHEAD() bool`

HasCommitMetaOfHEAD returns a boolean if a field has been set.

### GetCommonAncestorHash

`func (o *ReferenceMetadata) GetCommonAncestorHash() string`

GetCommonAncestorHash returns the CommonAncestorHash field if non-nil, zero value otherwise.

### GetCommonAncestorHashOk

`func (o *ReferenceMetadata) GetCommonAncestorHashOk() (*string, bool)`

GetCommonAncestorHashOk returns a tuple with the CommonAncestorHash field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetCommonAncestorHash

`func (o *ReferenceMetadata) SetCommonAncestorHash(v string)`

SetCommonAncestorHash sets CommonAncestorHash field to given value.

### HasCommonAncestorHash

`func (o *ReferenceMetadata) HasCommonAncestorHash() bool`

HasCommonAncestorHash returns a boolean if a field has been set.

### GetNumTotalCommits

`func (o *ReferenceMetadata) GetNumTotalCommits() int64`

GetNumTotalCommits returns the NumTotalCommits field if non-nil, zero value otherwise.

### GetNumTotalCommitsOk

`func (o *ReferenceMetadata) GetNumTotalCommitsOk() (*int64, bool)`

GetNumTotalCommitsOk returns a tuple with the NumTotalCommits field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetNumTotalCommits

`func (o *ReferenceMetadata) SetNumTotalCommits(v int64)`

SetNumTotalCommits sets NumTotalCommits field to given value.

### HasNumTotalCommits

`func (o *ReferenceMetadata) HasNumTotalCommits() bool`

HasNumTotalCommits returns a boolean if a field has been set.


[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


