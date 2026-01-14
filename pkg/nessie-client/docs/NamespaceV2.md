# NamespaceV2

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**Id** | Pointer to **string** |  | [optional] 
**Elements** | **[]string** |  | 
**Properties** | **map[string]string** |  | 

## Methods

### NewNamespaceV2

`func NewNamespaceV2(elements []string, properties map[string]string, ) *NamespaceV2`

NewNamespaceV2 instantiates a new NamespaceV2 object
This constructor will assign default values to properties that have it defined,
and makes sure properties required by API are set, but the set of arguments
will change when the set of required properties is changed

### NewNamespaceV2WithDefaults

`func NewNamespaceV2WithDefaults() *NamespaceV2`

NewNamespaceV2WithDefaults instantiates a new NamespaceV2 object
This constructor will only assign default values to properties that have it defined,
but it doesn't guarantee that properties required by API are set

### GetId

`func (o *NamespaceV2) GetId() string`

GetId returns the Id field if non-nil, zero value otherwise.

### GetIdOk

`func (o *NamespaceV2) GetIdOk() (*string, bool)`

GetIdOk returns a tuple with the Id field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetId

`func (o *NamespaceV2) SetId(v string)`

SetId sets Id field to given value.

### HasId

`func (o *NamespaceV2) HasId() bool`

HasId returns a boolean if a field has been set.

### GetElements

`func (o *NamespaceV2) GetElements() []string`

GetElements returns the Elements field if non-nil, zero value otherwise.

### GetElementsOk

`func (o *NamespaceV2) GetElementsOk() (*[]string, bool)`

GetElementsOk returns a tuple with the Elements field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetElements

`func (o *NamespaceV2) SetElements(v []string)`

SetElements sets Elements field to given value.


### GetProperties

`func (o *NamespaceV2) GetProperties() map[string]string`

GetProperties returns the Properties field if non-nil, zero value otherwise.

### GetPropertiesOk

`func (o *NamespaceV2) GetPropertiesOk() (*map[string]string, bool)`

GetPropertiesOk returns a tuple with the Properties field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetProperties

`func (o *NamespaceV2) SetProperties(v map[string]string)`

SetProperties sets Properties field to given value.



[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


