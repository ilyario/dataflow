# MergeKeyBehavior

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**Key** | Pointer to [**ContentKey**](ContentKey.md) |  | [optional] 
**MergeBehavior** | Pointer to [**MergeBehavior**](MergeBehavior.md) |  | [optional] 
**ExpectedTargetContent** | Pointer to [**Content**](Content.md) |  | [optional] 
**ResolvedContent** | Pointer to [**Content**](Content.md) |  | [optional] 
**ExpectedTargetDocumentation** | Pointer to [**Documentation**](Documentation.md) |  | [optional] 
**ResolvedDocumentation** | Pointer to [**Documentation**](Documentation.md) |  | [optional] 
**Metadata** | Pointer to [**[]ContentMetadata**](ContentMetadata.md) |  | [optional] 

## Methods

### NewMergeKeyBehavior

`func NewMergeKeyBehavior() *MergeKeyBehavior`

NewMergeKeyBehavior instantiates a new MergeKeyBehavior object
This constructor will assign default values to properties that have it defined,
and makes sure properties required by API are set, but the set of arguments
will change when the set of required properties is changed

### NewMergeKeyBehaviorWithDefaults

`func NewMergeKeyBehaviorWithDefaults() *MergeKeyBehavior`

NewMergeKeyBehaviorWithDefaults instantiates a new MergeKeyBehavior object
This constructor will only assign default values to properties that have it defined,
but it doesn't guarantee that properties required by API are set

### GetKey

`func (o *MergeKeyBehavior) GetKey() ContentKey`

GetKey returns the Key field if non-nil, zero value otherwise.

### GetKeyOk

`func (o *MergeKeyBehavior) GetKeyOk() (*ContentKey, bool)`

GetKeyOk returns a tuple with the Key field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetKey

`func (o *MergeKeyBehavior) SetKey(v ContentKey)`

SetKey sets Key field to given value.

### HasKey

`func (o *MergeKeyBehavior) HasKey() bool`

HasKey returns a boolean if a field has been set.

### GetMergeBehavior

`func (o *MergeKeyBehavior) GetMergeBehavior() MergeBehavior`

GetMergeBehavior returns the MergeBehavior field if non-nil, zero value otherwise.

### GetMergeBehaviorOk

`func (o *MergeKeyBehavior) GetMergeBehaviorOk() (*MergeBehavior, bool)`

GetMergeBehaviorOk returns a tuple with the MergeBehavior field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetMergeBehavior

`func (o *MergeKeyBehavior) SetMergeBehavior(v MergeBehavior)`

SetMergeBehavior sets MergeBehavior field to given value.

### HasMergeBehavior

`func (o *MergeKeyBehavior) HasMergeBehavior() bool`

HasMergeBehavior returns a boolean if a field has been set.

### GetExpectedTargetContent

`func (o *MergeKeyBehavior) GetExpectedTargetContent() Content`

GetExpectedTargetContent returns the ExpectedTargetContent field if non-nil, zero value otherwise.

### GetExpectedTargetContentOk

`func (o *MergeKeyBehavior) GetExpectedTargetContentOk() (*Content, bool)`

GetExpectedTargetContentOk returns a tuple with the ExpectedTargetContent field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetExpectedTargetContent

`func (o *MergeKeyBehavior) SetExpectedTargetContent(v Content)`

SetExpectedTargetContent sets ExpectedTargetContent field to given value.

### HasExpectedTargetContent

`func (o *MergeKeyBehavior) HasExpectedTargetContent() bool`

HasExpectedTargetContent returns a boolean if a field has been set.

### GetResolvedContent

`func (o *MergeKeyBehavior) GetResolvedContent() Content`

GetResolvedContent returns the ResolvedContent field if non-nil, zero value otherwise.

### GetResolvedContentOk

`func (o *MergeKeyBehavior) GetResolvedContentOk() (*Content, bool)`

GetResolvedContentOk returns a tuple with the ResolvedContent field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetResolvedContent

`func (o *MergeKeyBehavior) SetResolvedContent(v Content)`

SetResolvedContent sets ResolvedContent field to given value.

### HasResolvedContent

`func (o *MergeKeyBehavior) HasResolvedContent() bool`

HasResolvedContent returns a boolean if a field has been set.

### GetExpectedTargetDocumentation

`func (o *MergeKeyBehavior) GetExpectedTargetDocumentation() Documentation`

GetExpectedTargetDocumentation returns the ExpectedTargetDocumentation field if non-nil, zero value otherwise.

### GetExpectedTargetDocumentationOk

`func (o *MergeKeyBehavior) GetExpectedTargetDocumentationOk() (*Documentation, bool)`

GetExpectedTargetDocumentationOk returns a tuple with the ExpectedTargetDocumentation field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetExpectedTargetDocumentation

`func (o *MergeKeyBehavior) SetExpectedTargetDocumentation(v Documentation)`

SetExpectedTargetDocumentation sets ExpectedTargetDocumentation field to given value.

### HasExpectedTargetDocumentation

`func (o *MergeKeyBehavior) HasExpectedTargetDocumentation() bool`

HasExpectedTargetDocumentation returns a boolean if a field has been set.

### GetResolvedDocumentation

`func (o *MergeKeyBehavior) GetResolvedDocumentation() Documentation`

GetResolvedDocumentation returns the ResolvedDocumentation field if non-nil, zero value otherwise.

### GetResolvedDocumentationOk

`func (o *MergeKeyBehavior) GetResolvedDocumentationOk() (*Documentation, bool)`

GetResolvedDocumentationOk returns a tuple with the ResolvedDocumentation field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetResolvedDocumentation

`func (o *MergeKeyBehavior) SetResolvedDocumentation(v Documentation)`

SetResolvedDocumentation sets ResolvedDocumentation field to given value.

### HasResolvedDocumentation

`func (o *MergeKeyBehavior) HasResolvedDocumentation() bool`

HasResolvedDocumentation returns a boolean if a field has been set.

### GetMetadata

`func (o *MergeKeyBehavior) GetMetadata() []ContentMetadata`

GetMetadata returns the Metadata field if non-nil, zero value otherwise.

### GetMetadataOk

`func (o *MergeKeyBehavior) GetMetadataOk() (*[]ContentMetadata, bool)`

GetMetadataOk returns a tuple with the Metadata field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetMetadata

`func (o *MergeKeyBehavior) SetMetadata(v []ContentMetadata)`

SetMetadata sets Metadata field to given value.

### HasMetadata

`func (o *MergeKeyBehavior) HasMetadata() bool`

HasMetadata returns a boolean if a field has been set.


[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


