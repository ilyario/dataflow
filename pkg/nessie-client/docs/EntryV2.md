# EntryV2

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**Type** | Pointer to **string** | Declares the type of a Nessie content object, which is currently one of ICEBERG_TABLE, DELTA_LAKE_TABLE, ICEBERG_VIEW, NAMESPACE or UDF, which are the discriminator mapping values of the &#39;Content&#39; type. | [optional] 
**Name** | [**ContentKeyV2**](ContentKeyV2.md) |  | 
**ContentId** | Pointer to **string** |  | [optional] 
**Content** | Pointer to [**ContentV2**](ContentV2.md) |  | [optional] 

## Methods

### NewEntryV2

`func NewEntryV2(name ContentKeyV2, ) *EntryV2`

NewEntryV2 instantiates a new EntryV2 object
This constructor will assign default values to properties that have it defined,
and makes sure properties required by API are set, but the set of arguments
will change when the set of required properties is changed

### NewEntryV2WithDefaults

`func NewEntryV2WithDefaults() *EntryV2`

NewEntryV2WithDefaults instantiates a new EntryV2 object
This constructor will only assign default values to properties that have it defined,
but it doesn't guarantee that properties required by API are set

### GetType

`func (o *EntryV2) GetType() string`

GetType returns the Type field if non-nil, zero value otherwise.

### GetTypeOk

`func (o *EntryV2) GetTypeOk() (*string, bool)`

GetTypeOk returns a tuple with the Type field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetType

`func (o *EntryV2) SetType(v string)`

SetType sets Type field to given value.

### HasType

`func (o *EntryV2) HasType() bool`

HasType returns a boolean if a field has been set.

### GetName

`func (o *EntryV2) GetName() ContentKeyV2`

GetName returns the Name field if non-nil, zero value otherwise.

### GetNameOk

`func (o *EntryV2) GetNameOk() (*ContentKeyV2, bool)`

GetNameOk returns a tuple with the Name field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetName

`func (o *EntryV2) SetName(v ContentKeyV2)`

SetName sets Name field to given value.


### GetContentId

`func (o *EntryV2) GetContentId() string`

GetContentId returns the ContentId field if non-nil, zero value otherwise.

### GetContentIdOk

`func (o *EntryV2) GetContentIdOk() (*string, bool)`

GetContentIdOk returns a tuple with the ContentId field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetContentId

`func (o *EntryV2) SetContentId(v string)`

SetContentId sets ContentId field to given value.

### HasContentId

`func (o *EntryV2) HasContentId() bool`

HasContentId returns a boolean if a field has been set.

### GetContent

`func (o *EntryV2) GetContent() ContentV2`

GetContent returns the Content field if non-nil, zero value otherwise.

### GetContentOk

`func (o *EntryV2) GetContentOk() (*ContentV2, bool)`

GetContentOk returns a tuple with the Content field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetContent

`func (o *EntryV2) SetContent(v ContentV2)`

SetContent sets Content field to given value.

### HasContent

`func (o *EntryV2) HasContent() bool`

HasContent returns a boolean if a field has been set.


[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


