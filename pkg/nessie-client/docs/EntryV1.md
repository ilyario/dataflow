# EntryV1

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**Type** | Pointer to **string** | Declares the type of a Nessie content object, which is currently one of ICEBERG_TABLE, DELTA_LAKE_TABLE, ICEBERG_VIEW, NAMESPACE or UDF, which are the discriminator mapping values of the &#39;Content&#39; type. | [optional] 
**Name** | [**ContentKeyV1**](ContentKeyV1.md) |  | 

## Methods

### NewEntryV1

`func NewEntryV1(name ContentKeyV1, ) *EntryV1`

NewEntryV1 instantiates a new EntryV1 object
This constructor will assign default values to properties that have it defined,
and makes sure properties required by API are set, but the set of arguments
will change when the set of required properties is changed

### NewEntryV1WithDefaults

`func NewEntryV1WithDefaults() *EntryV1`

NewEntryV1WithDefaults instantiates a new EntryV1 object
This constructor will only assign default values to properties that have it defined,
but it doesn't guarantee that properties required by API are set

### GetType

`func (o *EntryV1) GetType() string`

GetType returns the Type field if non-nil, zero value otherwise.

### GetTypeOk

`func (o *EntryV1) GetTypeOk() (*string, bool)`

GetTypeOk returns a tuple with the Type field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetType

`func (o *EntryV1) SetType(v string)`

SetType sets Type field to given value.

### HasType

`func (o *EntryV1) HasType() bool`

HasType returns a boolean if a field has been set.

### GetName

`func (o *EntryV1) GetName() ContentKeyV1`

GetName returns the Name field if non-nil, zero value otherwise.

### GetNameOk

`func (o *EntryV1) GetNameOk() (*ContentKeyV1, bool)`

GetNameOk returns a tuple with the Name field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetName

`func (o *EntryV1) SetName(v ContentKeyV1)`

SetName sets Name field to given value.



[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


