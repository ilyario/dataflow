# \V2API

All URIs are relative to *http://localhost:19120*

Method | HTTP request | Description
------------- | ------------- | -------------
[**AssignReferenceV2**](V2API.md#AssignReferenceV2) | **Put** /v2/trees/{ref} | Set a named reference to a specific hash via another reference.
[**CommitV2**](V2API.md#CommitV2) | **Post** /v2/trees/{branch}/history/commit | Commit one or more operations against the given &#39;branch&#39;.
[**CreateReferenceV2**](V2API.md#CreateReferenceV2) | **Post** /v2/trees | Create a new branch or tag
[**DeleteReferenceV2**](V2API.md#DeleteReferenceV2) | **Delete** /v2/trees/{ref} | Delete a reference
[**GetAllReferencesV2**](V2API.md#GetAllReferencesV2) | **Get** /v2/trees | Get information about all branches and tags
[**GetCommitLogV2**](V2API.md#GetCommitLogV2) | **Get** /v2/trees/{ref}/history | Get commit log for a particular reference
[**GetConfigV2**](V2API.md#GetConfigV2) | **Get** /v2/config | Returns repository and server settings relevant to clients.
[**GetContentV2**](V2API.md#GetContentV2) | **Get** /v2/trees/{ref}/contents/{key} | Get the content object associated with a key.
[**GetDiffV2**](V2API.md#GetDiffV2) | **Get** /v2/trees/{from-ref}/diff/{to-ref} | Get contents that differ in the trees specified by the two given references
[**GetEntriesV2**](V2API.md#GetEntriesV2) | **Get** /v2/trees/{ref}/entries | Fetch all entries for a given reference
[**GetMultipleContentsV2**](V2API.md#GetMultipleContentsV2) | **Post** /v2/trees/{ref}/contents | Get multiple content objects.
[**GetReferenceByNameV2**](V2API.md#GetReferenceByNameV2) | **Get** /v2/trees/{ref} | Fetch details of a reference
[**GetReferenceHistory**](V2API.md#GetReferenceHistory) | **Get** /v2/trees/{ref}/recent-changes | Fetch recent pointer changes of a reference
[**GetRepositoryConfig**](V2API.md#GetRepositoryConfig) | **Get** /v2/config/repository | Returns repository configurations of the requested types.
[**GetSeveralContents**](V2API.md#GetSeveralContents) | **Get** /v2/trees/{ref}/contents | Get multiple content objects.
[**MergeV2**](V2API.md#MergeV2) | **Post** /v2/trees/{branch}/history/merge | Merge commits from another reference onto &#39;branch&#39;.
[**TransplantV2**](V2API.md#TransplantV2) | **Post** /v2/trees/{branch}/history/transplant | Transplant commits specified by the &#39;Transplant&#39; payload object onto the given &#39;branch&#39;
[**UpdateRepositoryConfig**](V2API.md#UpdateRepositoryConfig) | **Post** /v2/config/repository | Create or update a repository configuration.



## AssignReferenceV2

> SingleReferenceResponseV2 AssignReferenceV2(ctx, ref).Reference(reference).Type_(type_).Execute()

Set a named reference to a specific hash via another reference.



### Example

```go
package main

import (
	"context"
	"fmt"
	"os"
	openapiclient "github.com/GIT_USER_ID/GIT_REPO_ID"
)

func main() {
	ref := TODO // interface{} | Specifies a named branch or tag reference with its expected HEAD 'hash' value.  For example: - name@hash - Identifies the 'hash' commit on a branch or tag.  The specified 'hash' must be the value of the current HEAD of the branch or tag known by the client. It will be used to validate that at execution time the reference points to the same hash that the caller expected when the operation arguments were constructed. 
	reference := openapiclient.Reference{Branch: openapiclient.NewBranch("Name_example")} // Reference | Reference to which the 'ref' (from the path parameter) shall be assigned. This must be either a 'Detached' commit, 'Branch' or 'Tag' via which the hash is visible to the caller.
	type_ := "type__example" // string | Optional expected type of the reference being reassigned (optional)

	configuration := openapiclient.NewConfiguration()
	apiClient := openapiclient.NewAPIClient(configuration)
	resp, r, err := apiClient.V2API.AssignReferenceV2(context.Background(), ref).Reference(reference).Type_(type_).Execute()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error when calling `V2API.AssignReferenceV2``: %v\n", err)
		fmt.Fprintf(os.Stderr, "Full HTTP response: %v\n", r)
	}
	// response from `AssignReferenceV2`: SingleReferenceResponseV2
	fmt.Fprintf(os.Stdout, "Response from `V2API.AssignReferenceV2`: %v\n", resp)
}
```

### Path Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
**ctx** | **context.Context** | context for authentication, logging, cancellation, deadlines, tracing, etc.
**ref** | [**interface{}**](.md) | Specifies a named branch or tag reference with its expected HEAD &#39;hash&#39; value.  For example: - name@hash - Identifies the &#39;hash&#39; commit on a branch or tag.  The specified &#39;hash&#39; must be the value of the current HEAD of the branch or tag known by the client. It will be used to validate that at execution time the reference points to the same hash that the caller expected when the operation arguments were constructed.  | 

### Other Parameters

Other parameters are passed through a pointer to a apiAssignReferenceV2Request struct via the builder pattern


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------

 **reference** | [**Reference**](Reference.md) | Reference to which the &#39;ref&#39; (from the path parameter) shall be assigned. This must be either a &#39;Detached&#39; commit, &#39;Branch&#39; or &#39;Tag&#39; via which the hash is visible to the caller. | 
 **type_** | **string** | Optional expected type of the reference being reassigned | 

### Return type

[**SingleReferenceResponseV2**](SingleReferenceResponseV2.md)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints)
[[Back to Model list]](../README.md#documentation-for-models)
[[Back to README]](../README.md)


## CommitV2

> CommitResponseV2 CommitV2(ctx, branch).Operations(operations).Execute()

Commit one or more operations against the given 'branch'.



### Example

```go
package main

import (
	"context"
	"fmt"
	"os"
	openapiclient "github.com/GIT_USER_ID/GIT_REPO_ID"
)

func main() {
	branch := TODO // interface{} | A reference to a particular version of the contents tree (a point in history) on a branch. This reference is specified in this form: - name@hash - Identifies the 'hash' commit on the named branch.  The 'hash' commit must be reachable from the current HEAD of the branch. In this case 'hash' indicates the state of contents that should be used for validating incoming changes. 
	operations := *openapiclient.NewOperations(*openapiclient.NewCommitMeta([]string{"Authors_example"}, []string{"AllSignedOffBy_example"}, "Message_example", map[string]string{"key": "Inner_example"}, map[string][]string{"key": []string{"Inner_example"}}, []string{"ParentCommitHashes_example"}), []openapiclient.Operation{openapiclient.Operation{Delete: openapiclient.NewDelete(*openapiclient.NewContentKey([]string{"Elements_example"}))}}) // Operations | Operations to commit

	configuration := openapiclient.NewConfiguration()
	apiClient := openapiclient.NewAPIClient(configuration)
	resp, r, err := apiClient.V2API.CommitV2(context.Background(), branch).Operations(operations).Execute()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error when calling `V2API.CommitV2``: %v\n", err)
		fmt.Fprintf(os.Stderr, "Full HTTP response: %v\n", r)
	}
	// response from `CommitV2`: CommitResponseV2
	fmt.Fprintf(os.Stdout, "Response from `V2API.CommitV2`: %v\n", resp)
}
```

### Path Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
**ctx** | **context.Context** | context for authentication, logging, cancellation, deadlines, tracing, etc.
**branch** | [**interface{}**](.md) | A reference to a particular version of the contents tree (a point in history) on a branch. This reference is specified in this form: - name@hash - Identifies the &#39;hash&#39; commit on the named branch.  The &#39;hash&#39; commit must be reachable from the current HEAD of the branch. In this case &#39;hash&#39; indicates the state of contents that should be used for validating incoming changes.  | 

### Other Parameters

Other parameters are passed through a pointer to a apiCommitV2Request struct via the builder pattern


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------

 **operations** | [**Operations**](Operations.md) | Operations to commit | 

### Return type

[**CommitResponseV2**](CommitResponseV2.md)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints)
[[Back to Model list]](../README.md#documentation-for-models)
[[Back to README]](../README.md)


## CreateReferenceV2

> SingleReferenceResponseV2 CreateReferenceV2(ctx).Name(name).Type_(type_).Reference(reference).Execute()

Create a new branch or tag



### Example

```go
package main

import (
	"context"
	"fmt"
	"os"
	openapiclient "github.com/GIT_USER_ID/GIT_REPO_ID"
)

func main() {
	name := "name_example" // string | A reference name.  Reference name must start with a letter, followed by letters, digits, one of the ./_- characters, not end with a slash or dot, not contain '..' 
	type_ := "type__example" // string | Type of the reference to be created
	reference := openapiclient.Reference{Branch: openapiclient.NewBranch("Name_example")} // Reference | Source reference data from which the new reference is to be created.

	configuration := openapiclient.NewConfiguration()
	apiClient := openapiclient.NewAPIClient(configuration)
	resp, r, err := apiClient.V2API.CreateReferenceV2(context.Background()).Name(name).Type_(type_).Reference(reference).Execute()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error when calling `V2API.CreateReferenceV2``: %v\n", err)
		fmt.Fprintf(os.Stderr, "Full HTTP response: %v\n", r)
	}
	// response from `CreateReferenceV2`: SingleReferenceResponseV2
	fmt.Fprintf(os.Stdout, "Response from `V2API.CreateReferenceV2`: %v\n", resp)
}
```

### Path Parameters



### Other Parameters

Other parameters are passed through a pointer to a apiCreateReferenceV2Request struct via the builder pattern


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **name** | **string** | A reference name.  Reference name must start with a letter, followed by letters, digits, one of the ./_- characters, not end with a slash or dot, not contain &#39;..&#39;  | 
 **type_** | **string** | Type of the reference to be created | 
 **reference** | [**Reference**](Reference.md) | Source reference data from which the new reference is to be created. | 

### Return type

[**SingleReferenceResponseV2**](SingleReferenceResponseV2.md)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints)
[[Back to Model list]](../README.md#documentation-for-models)
[[Back to README]](../README.md)


## DeleteReferenceV2

> SingleReferenceResponseV2 DeleteReferenceV2(ctx, ref).Type_(type_).Execute()

Delete a reference



### Example

```go
package main

import (
	"context"
	"fmt"
	"os"
	openapiclient "github.com/GIT_USER_ID/GIT_REPO_ID"
)

func main() {
	ref := TODO // interface{} | Specifies a named branch or tag reference with its expected HEAD 'hash' value.  For example: - name@hash - Identifies the 'hash' commit on a branch or tag.  The specified 'hash' must be the value of the current HEAD of the branch or tag known by the client. It will be used to validate that at execution time the reference points to the same hash that the caller expected when the operation arguments were constructed. 
	type_ := "type__example" // string | Optional expected type of the reference being deleted (optional)

	configuration := openapiclient.NewConfiguration()
	apiClient := openapiclient.NewAPIClient(configuration)
	resp, r, err := apiClient.V2API.DeleteReferenceV2(context.Background(), ref).Type_(type_).Execute()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error when calling `V2API.DeleteReferenceV2``: %v\n", err)
		fmt.Fprintf(os.Stderr, "Full HTTP response: %v\n", r)
	}
	// response from `DeleteReferenceV2`: SingleReferenceResponseV2
	fmt.Fprintf(os.Stdout, "Response from `V2API.DeleteReferenceV2`: %v\n", resp)
}
```

### Path Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
**ctx** | **context.Context** | context for authentication, logging, cancellation, deadlines, tracing, etc.
**ref** | [**interface{}**](.md) | Specifies a named branch or tag reference with its expected HEAD &#39;hash&#39; value.  For example: - name@hash - Identifies the &#39;hash&#39; commit on a branch or tag.  The specified &#39;hash&#39; must be the value of the current HEAD of the branch or tag known by the client. It will be used to validate that at execution time the reference points to the same hash that the caller expected when the operation arguments were constructed.  | 

### Other Parameters

Other parameters are passed through a pointer to a apiDeleteReferenceV2Request struct via the builder pattern


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------

 **type_** | **string** | Optional expected type of the reference being deleted | 

### Return type

[**SingleReferenceResponseV2**](SingleReferenceResponseV2.md)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints)
[[Back to Model list]](../README.md#documentation-for-models)
[[Back to README]](../README.md)


## GetAllReferencesV2

> ReferencesResponseV2 GetAllReferencesV2(ctx).Fetch(fetch).Filter(filter).MaxRecords(maxRecords).PageToken(pageToken).Execute()

Get information about all branches and tags

### Example

```go
package main

import (
	"context"
	"fmt"
	"os"
	openapiclient "github.com/GIT_USER_ID/GIT_REPO_ID"
)

func main() {
	fetch := openapiclient.FetchOption("MINIMAL") // FetchOption | Specifies how much extra information is to be retrived from the server.  If the fetch option is set to 'ALL' the following addition information will be returned for each Branch object in the output:  - numCommitsAhead (number of commits ahead of the default branch)  - numCommitsBehind (number of commits behind the default branch)  - commitMetaOfHEAD (the commit metadata of the HEAD commit)  - commonAncestorHash (the hash of the common ancestor in relation to the default branch).  - numTotalCommits (the total number of commits from the root to the HEAD of the branch).  The returned Tag instances will only contain the 'commitMetaOfHEAD' and 'numTotalCommits' fields.  Note that computing & fetching additional metadata might be computationally expensive on the server-side, so this flag should be used with care. (optional)
	filter := "filter_example" // string | A Common Expression Language (CEL) expression. An intro to CEL can be found at https://github.com/google/cel-spec/blob/master/doc/intro.md. Usable variables within the expression are:  - ref (Reference) describes the reference, with fields name (String), hash (String), metadata (ReferenceMetadata)  - metadata (ReferenceMetadata) shortcut to ref.metadata, never null, but possibly empty  - commit (CommitMeta) - shortcut to ref.metadata.commitMetaOfHEAD, never null, but possibly empty  - refType (String) - the reference type, either BRANCH or TAG  Note that the expression can only test attributes metadata and commit, if 'fetchOption' is set to 'ALL'. (optional)
	maxRecords := int32(56) // int32 | maximum number of entries to return, just a hint for the server (optional)
	pageToken := "pageToken_example" // string | paging continuation token, as returned in the previous value of the field 'token' in the corresponding 'EntriesResponse' or 'LogResponse' or 'ReferencesResponse' or 'RefLogResponse'. (optional)

	configuration := openapiclient.NewConfiguration()
	apiClient := openapiclient.NewAPIClient(configuration)
	resp, r, err := apiClient.V2API.GetAllReferencesV2(context.Background()).Fetch(fetch).Filter(filter).MaxRecords(maxRecords).PageToken(pageToken).Execute()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error when calling `V2API.GetAllReferencesV2``: %v\n", err)
		fmt.Fprintf(os.Stderr, "Full HTTP response: %v\n", r)
	}
	// response from `GetAllReferencesV2`: ReferencesResponseV2
	fmt.Fprintf(os.Stdout, "Response from `V2API.GetAllReferencesV2`: %v\n", resp)
}
```

### Path Parameters



### Other Parameters

Other parameters are passed through a pointer to a apiGetAllReferencesV2Request struct via the builder pattern


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **fetch** | [**FetchOption**](FetchOption.md) | Specifies how much extra information is to be retrived from the server.  If the fetch option is set to &#39;ALL&#39; the following addition information will be returned for each Branch object in the output:  - numCommitsAhead (number of commits ahead of the default branch)  - numCommitsBehind (number of commits behind the default branch)  - commitMetaOfHEAD (the commit metadata of the HEAD commit)  - commonAncestorHash (the hash of the common ancestor in relation to the default branch).  - numTotalCommits (the total number of commits from the root to the HEAD of the branch).  The returned Tag instances will only contain the &#39;commitMetaOfHEAD&#39; and &#39;numTotalCommits&#39; fields.  Note that computing &amp; fetching additional metadata might be computationally expensive on the server-side, so this flag should be used with care. | 
 **filter** | **string** | A Common Expression Language (CEL) expression. An intro to CEL can be found at https://github.com/google/cel-spec/blob/master/doc/intro.md. Usable variables within the expression are:  - ref (Reference) describes the reference, with fields name (String), hash (String), metadata (ReferenceMetadata)  - metadata (ReferenceMetadata) shortcut to ref.metadata, never null, but possibly empty  - commit (CommitMeta) - shortcut to ref.metadata.commitMetaOfHEAD, never null, but possibly empty  - refType (String) - the reference type, either BRANCH or TAG  Note that the expression can only test attributes metadata and commit, if &#39;fetchOption&#39; is set to &#39;ALL&#39;. | 
 **maxRecords** | **int32** | maximum number of entries to return, just a hint for the server | 
 **pageToken** | **string** | paging continuation token, as returned in the previous value of the field &#39;token&#39; in the corresponding &#39;EntriesResponse&#39; or &#39;LogResponse&#39; or &#39;ReferencesResponse&#39; or &#39;RefLogResponse&#39;. | 

### Return type

[**ReferencesResponseV2**](ReferencesResponseV2.md)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints)
[[Back to Model list]](../README.md#documentation-for-models)
[[Back to README]](../README.md)


## GetCommitLogV2

> LogResponseV2 GetCommitLogV2(ctx, ref).Fetch(fetch).Filter(filter).LimitHash(limitHash).MaxRecords(maxRecords).PageToken(pageToken).Execute()

Get commit log for a particular reference



### Example

```go
package main

import (
	"context"
	"fmt"
	"os"
	openapiclient "github.com/GIT_USER_ID/GIT_REPO_ID"
)

func main() {
	ref := TODO // interface{} | A reference to a particular version of the contents tree (a point in history).  Reference representations consist of: - The reference name. '-' means the default branch name. - A commit hash prefixed with '@'. - A relative commit specification. '~N' means the N-th predecessor commit, '*T' means the commit for which the timestamp T (milliseconds since epoch or ISO-8601 instant) is valid, '^N' means the N-th parentin a commit (N=2 is the merge parent).  If neither the reference name or the default branch name placeholder '-' is specified, the reference type 'DETACHED' will be assumed. If no commit hash is specified, the HEAD of the specified named reference will be used. An empty reference parameter is not valid.  This reference can be specified in these forms: - \\- (literal minus character) - identifies the HEAD of the default branch. - name - Identifies the HEAD commit of a branch or tag. - name@hash - Identifies the 'hash' commit on a branch or tag. - @hash - Identifies the 'hash' commit in an unspecified branch or tag. - -~3 - The 3rd predecessor commit from the HEAD of the default branch. - name~3 - The 3rd predecessor commit from the HEAD of a branch or tag. - @hash~3 - The 3rd predecessor commit of the 'hash' commit. - name@hash^2 - The merge parent of the 'hash' commit of a branch or tag. - @hash^2 - The merge parent of the 'hash' commit. - -*2021-04-07T14:42:25.534748Z - The predecessor commit closest to the HEAD of the default branch for the given ISO-8601 timestamp. - name*2021-04-07T14:42:25.534748Z - The predecessor commit closest to the HEAD of a branch or tag valid for the given ISO-8601 timestamp. - name*1685185847230 - The predecessor commit closest to the HEAD of a branch or tag valid for the given timestamp in milliseconds since epoch.  If both 'name' and 'hash' are given, 'hash' must be reachable from the current HEAD of the branch or tag. If 'name' is omitted, the reference will be of type 'DETACHED' (referencing a specific commit hash without claiming its reachability from any live HEAD). Using references of the last form may have authorization implications when compared to an equivalent reference of the former forms.  An empty reference parameter is invalid.  The 'name@hash' form always refers to the exact commit on a specific named reference. This is the most complete form of a reference. Other forms omit some of the details and require those gaps to be filled by the server at runtime. Although these forms may be convenient to a human-being, they may resolve differently at different times depending on the state of the system. Using the full 'name@hash' form is recommended to avoid ambiguity. 
	fetch := openapiclient.FetchOption("MINIMAL") // FetchOption | Specify how much information to be returned. Will fetch additional metadata such as parent commit hash and operations in a commit, for each commit if set to 'ALL'. (optional)
	filter := "filter_example" // string | A Common Expression Language (CEL) expression. An intro to CEL can be found at https://github.com/google/cel-spec/blob/master/doc/intro.md.  Usable variables within the expression are:  - 'commit' with fields 'author' (string), 'committer' (string), 'commitTime' (timestamp), 'hash' (string), ',message' (string), 'properties' (map)  - 'operations' (list), each operation has the fields 'type' (string, either 'PUT' or 'DELETE'), 'key' (string, namespace + table name), 'keyElements' (list of strings), 'namespace' (string), 'namespaceElements' (list of strings) and 'name' (string, the \"simple\" table name)  Note that the expression can only test against 'operations', if 'fetch' is set to 'ALL'.  Hint: when filtering commits, you can determine whether commits are \"missing\" (filtered) by checking whether 'LogEntry.parentCommitHash' is different from the hash of the previous commit in the log response. (optional)
	limitHash := "limitHash_example" // string | Hash on the given ref to identify the commit where the operation of fetching the log should stop, i.e. the 'far' end of the commit log, returned late in the result. (optional)
	maxRecords := int32(56) // int32 | maximum number of entries to return, just a hint for the server (optional)
	pageToken := "pageToken_example" // string | paging continuation token, as returned in the previous value of the field 'token' in the corresponding 'EntriesResponse' or 'LogResponse' or 'ReferencesResponse' or 'RefLogResponse'. (optional)

	configuration := openapiclient.NewConfiguration()
	apiClient := openapiclient.NewAPIClient(configuration)
	resp, r, err := apiClient.V2API.GetCommitLogV2(context.Background(), ref).Fetch(fetch).Filter(filter).LimitHash(limitHash).MaxRecords(maxRecords).PageToken(pageToken).Execute()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error when calling `V2API.GetCommitLogV2``: %v\n", err)
		fmt.Fprintf(os.Stderr, "Full HTTP response: %v\n", r)
	}
	// response from `GetCommitLogV2`: LogResponseV2
	fmt.Fprintf(os.Stdout, "Response from `V2API.GetCommitLogV2`: %v\n", resp)
}
```

### Path Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
**ctx** | **context.Context** | context for authentication, logging, cancellation, deadlines, tracing, etc.
**ref** | [**interface{}**](.md) | A reference to a particular version of the contents tree (a point in history).  Reference representations consist of: - The reference name. &#39;-&#39; means the default branch name. - A commit hash prefixed with &#39;@&#39;. - A relative commit specification. &#39;~N&#39; means the N-th predecessor commit, &#39;*T&#39; means the commit for which the timestamp T (milliseconds since epoch or ISO-8601 instant) is valid, &#39;^N&#39; means the N-th parentin a commit (N&#x3D;2 is the merge parent).  If neither the reference name or the default branch name placeholder &#39;-&#39; is specified, the reference type &#39;DETACHED&#39; will be assumed. If no commit hash is specified, the HEAD of the specified named reference will be used. An empty reference parameter is not valid.  This reference can be specified in these forms: - \\- (literal minus character) - identifies the HEAD of the default branch. - name - Identifies the HEAD commit of a branch or tag. - name@hash - Identifies the &#39;hash&#39; commit on a branch or tag. - @hash - Identifies the &#39;hash&#39; commit in an unspecified branch or tag. - -~3 - The 3rd predecessor commit from the HEAD of the default branch. - name~3 - The 3rd predecessor commit from the HEAD of a branch or tag. - @hash~3 - The 3rd predecessor commit of the &#39;hash&#39; commit. - name@hash^2 - The merge parent of the &#39;hash&#39; commit of a branch or tag. - @hash^2 - The merge parent of the &#39;hash&#39; commit. - -*2021-04-07T14:42:25.534748Z - The predecessor commit closest to the HEAD of the default branch for the given ISO-8601 timestamp. - name*2021-04-07T14:42:25.534748Z - The predecessor commit closest to the HEAD of a branch or tag valid for the given ISO-8601 timestamp. - name*1685185847230 - The predecessor commit closest to the HEAD of a branch or tag valid for the given timestamp in milliseconds since epoch.  If both &#39;name&#39; and &#39;hash&#39; are given, &#39;hash&#39; must be reachable from the current HEAD of the branch or tag. If &#39;name&#39; is omitted, the reference will be of type &#39;DETACHED&#39; (referencing a specific commit hash without claiming its reachability from any live HEAD). Using references of the last form may have authorization implications when compared to an equivalent reference of the former forms.  An empty reference parameter is invalid.  The &#39;name@hash&#39; form always refers to the exact commit on a specific named reference. This is the most complete form of a reference. Other forms omit some of the details and require those gaps to be filled by the server at runtime. Although these forms may be convenient to a human-being, they may resolve differently at different times depending on the state of the system. Using the full &#39;name@hash&#39; form is recommended to avoid ambiguity.  | 

### Other Parameters

Other parameters are passed through a pointer to a apiGetCommitLogV2Request struct via the builder pattern


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------

 **fetch** | [**FetchOption**](FetchOption.md) | Specify how much information to be returned. Will fetch additional metadata such as parent commit hash and operations in a commit, for each commit if set to &#39;ALL&#39;. | 
 **filter** | **string** | A Common Expression Language (CEL) expression. An intro to CEL can be found at https://github.com/google/cel-spec/blob/master/doc/intro.md.  Usable variables within the expression are:  - &#39;commit&#39; with fields &#39;author&#39; (string), &#39;committer&#39; (string), &#39;commitTime&#39; (timestamp), &#39;hash&#39; (string), &#39;,message&#39; (string), &#39;properties&#39; (map)  - &#39;operations&#39; (list), each operation has the fields &#39;type&#39; (string, either &#39;PUT&#39; or &#39;DELETE&#39;), &#39;key&#39; (string, namespace + table name), &#39;keyElements&#39; (list of strings), &#39;namespace&#39; (string), &#39;namespaceElements&#39; (list of strings) and &#39;name&#39; (string, the \&quot;simple\&quot; table name)  Note that the expression can only test against &#39;operations&#39;, if &#39;fetch&#39; is set to &#39;ALL&#39;.  Hint: when filtering commits, you can determine whether commits are \&quot;missing\&quot; (filtered) by checking whether &#39;LogEntry.parentCommitHash&#39; is different from the hash of the previous commit in the log response. | 
 **limitHash** | **string** | Hash on the given ref to identify the commit where the operation of fetching the log should stop, i.e. the &#39;far&#39; end of the commit log, returned late in the result. | 
 **maxRecords** | **int32** | maximum number of entries to return, just a hint for the server | 
 **pageToken** | **string** | paging continuation token, as returned in the previous value of the field &#39;token&#39; in the corresponding &#39;EntriesResponse&#39; or &#39;LogResponse&#39; or &#39;ReferencesResponse&#39; or &#39;RefLogResponse&#39;. | 

### Return type

[**LogResponseV2**](LogResponseV2.md)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints)
[[Back to Model list]](../README.md#documentation-for-models)
[[Back to README]](../README.md)


## GetConfigV2

> NessieConfigurationV2 GetConfigV2(ctx).Execute()

Returns repository and server settings relevant to clients.

### Example

```go
package main

import (
	"context"
	"fmt"
	"os"
	openapiclient "github.com/GIT_USER_ID/GIT_REPO_ID"
)

func main() {

	configuration := openapiclient.NewConfiguration()
	apiClient := openapiclient.NewAPIClient(configuration)
	resp, r, err := apiClient.V2API.GetConfigV2(context.Background()).Execute()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error when calling `V2API.GetConfigV2``: %v\n", err)
		fmt.Fprintf(os.Stderr, "Full HTTP response: %v\n", r)
	}
	// response from `GetConfigV2`: NessieConfigurationV2
	fmt.Fprintf(os.Stdout, "Response from `V2API.GetConfigV2`: %v\n", resp)
}
```

### Path Parameters

This endpoint does not need any parameter.

### Other Parameters

Other parameters are passed through a pointer to a apiGetConfigV2Request struct via the builder pattern


### Return type

[**NessieConfigurationV2**](NessieConfigurationV2.md)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints)
[[Back to Model list]](../README.md#documentation-for-models)
[[Back to README]](../README.md)


## GetContentV2

> ContentResponseV2 GetContentV2(ctx, key, ref).ForWrite(forWrite).WithDoc(withDoc).Execute()

Get the content object associated with a key.



### Example

```go
package main

import (
	"context"
	"fmt"
	"os"
	openapiclient "github.com/GIT_USER_ID/GIT_REPO_ID"
)

func main() {
	key := *openapiclient.NewContentKey([]string{"Elements_example"}) // ContentKey | The key to a content object.  Content key and namespace components are separated by the dot (`.`) character. The components itself must be escaped using the rules described in [NESSIE-SPEC-2.0.md in the repository](https://github.com/projectnessie/nessie/blob/main/api/NESSIE-SPEC-2-0.md).
	ref := TODO // interface{} | A reference to a particular version of the contents tree (a point in history).  Reference representations consist of: - The reference name. '-' means the default branch name. - A commit hash prefixed with '@'. - A relative commit specification. '~N' means the N-th predecessor commit, '*T' means the commit for which the timestamp T (milliseconds since epoch or ISO-8601 instant) is valid, '^N' means the N-th parentin a commit (N=2 is the merge parent).  If neither the reference name or the default branch name placeholder '-' is specified, the reference type 'DETACHED' will be assumed. If no commit hash is specified, the HEAD of the specified named reference will be used. An empty reference parameter is not valid.  This reference can be specified in these forms: - \\- (literal minus character) - identifies the HEAD of the default branch. - name - Identifies the HEAD commit of a branch or tag. - name@hash - Identifies the 'hash' commit on a branch or tag. - @hash - Identifies the 'hash' commit in an unspecified branch or tag. - -~3 - The 3rd predecessor commit from the HEAD of the default branch. - name~3 - The 3rd predecessor commit from the HEAD of a branch or tag. - @hash~3 - The 3rd predecessor commit of the 'hash' commit. - name@hash^2 - The merge parent of the 'hash' commit of a branch or tag. - @hash^2 - The merge parent of the 'hash' commit. - -*2021-04-07T14:42:25.534748Z - The predecessor commit closest to the HEAD of the default branch for the given ISO-8601 timestamp. - name*2021-04-07T14:42:25.534748Z - The predecessor commit closest to the HEAD of a branch or tag valid for the given ISO-8601 timestamp. - name*1685185847230 - The predecessor commit closest to the HEAD of a branch or tag valid for the given timestamp in milliseconds since epoch.  If both 'name' and 'hash' are given, 'hash' must be reachable from the current HEAD of the branch or tag. If 'name' is omitted, the reference will be of type 'DETACHED' (referencing a specific commit hash without claiming its reachability from any live HEAD). Using references of the last form may have authorization implications when compared to an equivalent reference of the former forms.  An empty reference parameter is invalid.  The 'name@hash' form always refers to the exact commit on a specific named reference. This is the most complete form of a reference. Other forms omit some of the details and require those gaps to be filled by the server at runtime. Although these forms may be convenient to a human-being, they may resolve differently at different times depending on the state of the system. Using the full 'name@hash' form is recommended to avoid ambiguity. 
	forWrite := true // bool | If set to 'true', access control checks will check for write/create privilege in addition to read privileges. (optional)
	withDoc := true // bool | Whether to return the documentation, if it exists. Default is to not return the documentation. (optional)

	configuration := openapiclient.NewConfiguration()
	apiClient := openapiclient.NewAPIClient(configuration)
	resp, r, err := apiClient.V2API.GetContentV2(context.Background(), key, ref).ForWrite(forWrite).WithDoc(withDoc).Execute()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error when calling `V2API.GetContentV2``: %v\n", err)
		fmt.Fprintf(os.Stderr, "Full HTTP response: %v\n", r)
	}
	// response from `GetContentV2`: ContentResponseV2
	fmt.Fprintf(os.Stdout, "Response from `V2API.GetContentV2`: %v\n", resp)
}
```

### Path Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
**ctx** | **context.Context** | context for authentication, logging, cancellation, deadlines, tracing, etc.
**key** | [**ContentKey**](.md) | The key to a content object.  Content key and namespace components are separated by the dot (&#x60;.&#x60;) character. The components itself must be escaped using the rules described in [NESSIE-SPEC-2.0.md in the repository](https://github.com/projectnessie/nessie/blob/main/api/NESSIE-SPEC-2-0.md). | 
**ref** | [**interface{}**](.md) | A reference to a particular version of the contents tree (a point in history).  Reference representations consist of: - The reference name. &#39;-&#39; means the default branch name. - A commit hash prefixed with &#39;@&#39;. - A relative commit specification. &#39;~N&#39; means the N-th predecessor commit, &#39;*T&#39; means the commit for which the timestamp T (milliseconds since epoch or ISO-8601 instant) is valid, &#39;^N&#39; means the N-th parentin a commit (N&#x3D;2 is the merge parent).  If neither the reference name or the default branch name placeholder &#39;-&#39; is specified, the reference type &#39;DETACHED&#39; will be assumed. If no commit hash is specified, the HEAD of the specified named reference will be used. An empty reference parameter is not valid.  This reference can be specified in these forms: - \\- (literal minus character) - identifies the HEAD of the default branch. - name - Identifies the HEAD commit of a branch or tag. - name@hash - Identifies the &#39;hash&#39; commit on a branch or tag. - @hash - Identifies the &#39;hash&#39; commit in an unspecified branch or tag. - -~3 - The 3rd predecessor commit from the HEAD of the default branch. - name~3 - The 3rd predecessor commit from the HEAD of a branch or tag. - @hash~3 - The 3rd predecessor commit of the &#39;hash&#39; commit. - name@hash^2 - The merge parent of the &#39;hash&#39; commit of a branch or tag. - @hash^2 - The merge parent of the &#39;hash&#39; commit. - -*2021-04-07T14:42:25.534748Z - The predecessor commit closest to the HEAD of the default branch for the given ISO-8601 timestamp. - name*2021-04-07T14:42:25.534748Z - The predecessor commit closest to the HEAD of a branch or tag valid for the given ISO-8601 timestamp. - name*1685185847230 - The predecessor commit closest to the HEAD of a branch or tag valid for the given timestamp in milliseconds since epoch.  If both &#39;name&#39; and &#39;hash&#39; are given, &#39;hash&#39; must be reachable from the current HEAD of the branch or tag. If &#39;name&#39; is omitted, the reference will be of type &#39;DETACHED&#39; (referencing a specific commit hash without claiming its reachability from any live HEAD). Using references of the last form may have authorization implications when compared to an equivalent reference of the former forms.  An empty reference parameter is invalid.  The &#39;name@hash&#39; form always refers to the exact commit on a specific named reference. This is the most complete form of a reference. Other forms omit some of the details and require those gaps to be filled by the server at runtime. Although these forms may be convenient to a human-being, they may resolve differently at different times depending on the state of the system. Using the full &#39;name@hash&#39; form is recommended to avoid ambiguity.  | 

### Other Parameters

Other parameters are passed through a pointer to a apiGetContentV2Request struct via the builder pattern


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------


 **forWrite** | **bool** | If set to &#39;true&#39;, access control checks will check for write/create privilege in addition to read privileges. | 
 **withDoc** | **bool** | Whether to return the documentation, if it exists. Default is to not return the documentation. | 

### Return type

[**ContentResponseV2**](ContentResponseV2.md)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints)
[[Back to Model list]](../README.md#documentation-for-models)
[[Back to README]](../README.md)


## GetDiffV2

> DiffResponseV2 GetDiffV2(ctx, fromRef, toRef).Filter(filter).Key(key).MaxKey(maxKey).MaxRecords(maxRecords).MinKey(minKey).PageToken(pageToken).PrefixKey(prefixKey).Execute()

Get contents that differ in the trees specified by the two given references



### Example

```go
package main

import (
	"context"
	"fmt"
	"os"
	openapiclient "github.com/GIT_USER_ID/GIT_REPO_ID"
)

func main() {
	fromRef := "fromRef_example" // string | A reference to a particular version of the contents tree (a point in history).  Reference representations consist of: - The reference name. '-' means the default branch name. - A commit hash prefixed with '@'. - A relative commit specification. '~N' means the N-th predecessor commit, '*T' means the commit for which the timestamp T (milliseconds since epoch or ISO-8601 instant) is valid, '^N' means the N-th parentin a commit (N=2 is the merge parent).  If neither the reference name or the default branch name placeholder '-' is specified, the reference type 'DETACHED' will be assumed. If no commit hash is specified, the HEAD of the specified named reference will be used. An empty reference parameter is not valid.  This reference can be specified in these forms: - \\- (literal minus character) - identifies the HEAD of the default branch. - name - Identifies the HEAD commit of a branch or tag. - name@hash - Identifies the 'hash' commit on a branch or tag. - @hash - Identifies the 'hash' commit in an unspecified branch or tag. - -~3 - The 3rd predecessor commit from the HEAD of the default branch. - name~3 - The 3rd predecessor commit from the HEAD of a branch or tag. - @hash~3 - The 3rd predecessor commit of the 'hash' commit. - name@hash^2 - The merge parent of the 'hash' commit of a branch or tag. - @hash^2 - The merge parent of the 'hash' commit. - -*2021-04-07T14:42:25.534748Z - The predecessor commit closest to the HEAD of the default branch for the given ISO-8601 timestamp. - name*2021-04-07T14:42:25.534748Z - The predecessor commit closest to the HEAD of a branch or tag valid for the given ISO-8601 timestamp. - name*1685185847230 - The predecessor commit closest to the HEAD of a branch or tag valid for the given timestamp in milliseconds since epoch.  If both 'name' and 'hash' are given, 'hash' must be reachable from the current HEAD of the branch or tag. If 'name' is omitted, the reference will be of type 'DETACHED' (referencing a specific commit hash without claiming its reachability from any live HEAD). Using references of the last form may have authorization implications when compared to an equivalent reference of the former forms.  An empty reference parameter is invalid.  The 'name@hash' form always refers to the exact commit on a specific named reference. This is the most complete form of a reference. Other forms omit some of the details and require those gaps to be filled by the server at runtime. Although these forms may be convenient to a human-being, they may resolve differently at different times depending on the state of the system. Using the full 'name@hash' form is recommended to avoid ambiguity. 
	toRef := "toRef_example" // string | Same reference spec as in the 'from-ref' parameter but identifying the other tree for comparison.
	filter := "filter_example" // string | A Common Expression Language (CEL) expression. An intro to CEL can be found at https://github.com/google/cel-spec/blob/master/doc/intro.md.  Usable variables within the expression are:  - 'key' (string, namespace + table name), 'keyElements' (list of strings), 'namespace' (string), 'namespaceElements' (list of strings) and 'name' (string, the \"simple\" table name) (optional)
	key := []openapiclient.ContentKey{*openapiclient.NewContentKey([]string{"Elements_example"})} // []ContentKey | Restrict the result to one or more keys.  Can be combined with min/max-key and prefix-key parameters, however both predicates must match. This means that keys specified via this parameter that do not match a given min/max-key or prefix-key will not be returned.  Content key and namespace components are separated by the dot (`.`) character. The components itself must be escaped using the rules described in [NESSIE-SPEC-2.0.md in the repository](https://github.com/projectnessie/nessie/blob/main/api/NESSIE-SPEC-2-0.md). (optional)
	maxKey := *openapiclient.NewContentKey([]string{"Elements_example"}) // ContentKey | The upper bound of the content key range to retrieve (inclusive). The content keys of all returned entries will be less than or equal to the max-value. Content-keys are compared as a 'whole', unlike prefix-keys.  Content key and namespace components are separated by the dot (`.`) character. The components itself must be escaped using the rules described in [NESSIE-SPEC-2.0.md in the repository](https://github.com/projectnessie/nessie/blob/main/api/NESSIE-SPEC-2-0.md). (optional)
	maxRecords := int32(56) // int32 | maximum number of entries to return, just a hint for the server (optional)
	minKey :=  // ContentKey | The lower bound of the content key range to retrieve (inclusive). The content keys of all returned entries will be greater than or equal to the min-value. Content-keys are compared as a 'whole', unlike prefix-keys.  Content key and namespace components are separated by the dot (`.`) character. The components itself must be escaped using the rules described in [NESSIE-SPEC-2.0.md in the repository](https://github.com/projectnessie/nessie/blob/main/api/NESSIE-SPEC-2-0.md). (optional)
	pageToken := "pageToken_example" // string | paging continuation token, as returned in the previous value of the field 'token' in the corresponding 'EntriesResponse' or 'LogResponse' or 'ReferencesResponse' or 'RefLogResponse'. (optional)
	prefixKey :=  // ContentKey | The content key prefix to retrieve (inclusive). A content key matches a given prefix, a content key's elements starts with all elements of the prefix-key. Key prefixes exactly match key-element boundaries.  Must not be combined with min/max-key parameters.  Content key and namespace components are separated by the dot (`.`) character. The components itself must be escaped using the rules described in [NESSIE-SPEC-2.0.md in the repository](https://github.com/projectnessie/nessie/blob/main/api/NESSIE-SPEC-2-0.md). (optional)

	configuration := openapiclient.NewConfiguration()
	apiClient := openapiclient.NewAPIClient(configuration)
	resp, r, err := apiClient.V2API.GetDiffV2(context.Background(), fromRef, toRef).Filter(filter).Key(key).MaxKey(maxKey).MaxRecords(maxRecords).MinKey(minKey).PageToken(pageToken).PrefixKey(prefixKey).Execute()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error when calling `V2API.GetDiffV2``: %v\n", err)
		fmt.Fprintf(os.Stderr, "Full HTTP response: %v\n", r)
	}
	// response from `GetDiffV2`: DiffResponseV2
	fmt.Fprintf(os.Stdout, "Response from `V2API.GetDiffV2`: %v\n", resp)
}
```

### Path Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
**ctx** | **context.Context** | context for authentication, logging, cancellation, deadlines, tracing, etc.
**fromRef** | **string** | A reference to a particular version of the contents tree (a point in history).  Reference representations consist of: - The reference name. &#39;-&#39; means the default branch name. - A commit hash prefixed with &#39;@&#39;. - A relative commit specification. &#39;~N&#39; means the N-th predecessor commit, &#39;*T&#39; means the commit for which the timestamp T (milliseconds since epoch or ISO-8601 instant) is valid, &#39;^N&#39; means the N-th parentin a commit (N&#x3D;2 is the merge parent).  If neither the reference name or the default branch name placeholder &#39;-&#39; is specified, the reference type &#39;DETACHED&#39; will be assumed. If no commit hash is specified, the HEAD of the specified named reference will be used. An empty reference parameter is not valid.  This reference can be specified in these forms: - \\- (literal minus character) - identifies the HEAD of the default branch. - name - Identifies the HEAD commit of a branch or tag. - name@hash - Identifies the &#39;hash&#39; commit on a branch or tag. - @hash - Identifies the &#39;hash&#39; commit in an unspecified branch or tag. - -~3 - The 3rd predecessor commit from the HEAD of the default branch. - name~3 - The 3rd predecessor commit from the HEAD of a branch or tag. - @hash~3 - The 3rd predecessor commit of the &#39;hash&#39; commit. - name@hash^2 - The merge parent of the &#39;hash&#39; commit of a branch or tag. - @hash^2 - The merge parent of the &#39;hash&#39; commit. - -*2021-04-07T14:42:25.534748Z - The predecessor commit closest to the HEAD of the default branch for the given ISO-8601 timestamp. - name*2021-04-07T14:42:25.534748Z - The predecessor commit closest to the HEAD of a branch or tag valid for the given ISO-8601 timestamp. - name*1685185847230 - The predecessor commit closest to the HEAD of a branch or tag valid for the given timestamp in milliseconds since epoch.  If both &#39;name&#39; and &#39;hash&#39; are given, &#39;hash&#39; must be reachable from the current HEAD of the branch or tag. If &#39;name&#39; is omitted, the reference will be of type &#39;DETACHED&#39; (referencing a specific commit hash without claiming its reachability from any live HEAD). Using references of the last form may have authorization implications when compared to an equivalent reference of the former forms.  An empty reference parameter is invalid.  The &#39;name@hash&#39; form always refers to the exact commit on a specific named reference. This is the most complete form of a reference. Other forms omit some of the details and require those gaps to be filled by the server at runtime. Although these forms may be convenient to a human-being, they may resolve differently at different times depending on the state of the system. Using the full &#39;name@hash&#39; form is recommended to avoid ambiguity.  | 
**toRef** | **string** | Same reference spec as in the &#39;from-ref&#39; parameter but identifying the other tree for comparison. | 

### Other Parameters

Other parameters are passed through a pointer to a apiGetDiffV2Request struct via the builder pattern


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------


 **filter** | **string** | A Common Expression Language (CEL) expression. An intro to CEL can be found at https://github.com/google/cel-spec/blob/master/doc/intro.md.  Usable variables within the expression are:  - &#39;key&#39; (string, namespace + table name), &#39;keyElements&#39; (list of strings), &#39;namespace&#39; (string), &#39;namespaceElements&#39; (list of strings) and &#39;name&#39; (string, the \&quot;simple\&quot; table name) | 
 **key** | [**[]ContentKey**](ContentKey.md) | Restrict the result to one or more keys.  Can be combined with min/max-key and prefix-key parameters, however both predicates must match. This means that keys specified via this parameter that do not match a given min/max-key or prefix-key will not be returned.  Content key and namespace components are separated by the dot (&#x60;.&#x60;) character. The components itself must be escaped using the rules described in [NESSIE-SPEC-2.0.md in the repository](https://github.com/projectnessie/nessie/blob/main/api/NESSIE-SPEC-2-0.md). | 
 **maxKey** | [**ContentKey**](ContentKey.md) | The upper bound of the content key range to retrieve (inclusive). The content keys of all returned entries will be less than or equal to the max-value. Content-keys are compared as a &#39;whole&#39;, unlike prefix-keys.  Content key and namespace components are separated by the dot (&#x60;.&#x60;) character. The components itself must be escaped using the rules described in [NESSIE-SPEC-2.0.md in the repository](https://github.com/projectnessie/nessie/blob/main/api/NESSIE-SPEC-2-0.md). | 
 **maxRecords** | **int32** | maximum number of entries to return, just a hint for the server | 
 **minKey** | [**ContentKey**](ContentKey.md) | The lower bound of the content key range to retrieve (inclusive). The content keys of all returned entries will be greater than or equal to the min-value. Content-keys are compared as a &#39;whole&#39;, unlike prefix-keys.  Content key and namespace components are separated by the dot (&#x60;.&#x60;) character. The components itself must be escaped using the rules described in [NESSIE-SPEC-2.0.md in the repository](https://github.com/projectnessie/nessie/blob/main/api/NESSIE-SPEC-2-0.md). | 
 **pageToken** | **string** | paging continuation token, as returned in the previous value of the field &#39;token&#39; in the corresponding &#39;EntriesResponse&#39; or &#39;LogResponse&#39; or &#39;ReferencesResponse&#39; or &#39;RefLogResponse&#39;. | 
 **prefixKey** | [**ContentKey**](ContentKey.md) | The content key prefix to retrieve (inclusive). A content key matches a given prefix, a content key&#39;s elements starts with all elements of the prefix-key. Key prefixes exactly match key-element boundaries.  Must not be combined with min/max-key parameters.  Content key and namespace components are separated by the dot (&#x60;.&#x60;) character. The components itself must be escaped using the rules described in [NESSIE-SPEC-2.0.md in the repository](https://github.com/projectnessie/nessie/blob/main/api/NESSIE-SPEC-2-0.md). | 

### Return type

[**DiffResponseV2**](DiffResponseV2.md)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints)
[[Back to Model list]](../README.md#documentation-for-models)
[[Back to README]](../README.md)


## GetEntriesV2

> EntriesResponseV2 GetEntriesV2(ctx, ref).Content(content).Filter(filter).Key(key).MaxKey(maxKey).MaxRecords(maxRecords).MinKey(minKey).PageToken(pageToken).PrefixKey(prefixKey).Execute()

Fetch all entries for a given reference



### Example

```go
package main

import (
	"context"
	"fmt"
	"os"
	openapiclient "github.com/GIT_USER_ID/GIT_REPO_ID"
)

func main() {
	ref := TODO // interface{} | A reference to a particular version of the contents tree (a point in history).  Reference representations consist of: - The reference name. '-' means the default branch name. - A commit hash prefixed with '@'. - A relative commit specification. '~N' means the N-th predecessor commit, '*T' means the commit for which the timestamp T (milliseconds since epoch or ISO-8601 instant) is valid, '^N' means the N-th parentin a commit (N=2 is the merge parent).  If neither the reference name or the default branch name placeholder '-' is specified, the reference type 'DETACHED' will be assumed. If no commit hash is specified, the HEAD of the specified named reference will be used. An empty reference parameter is not valid.  This reference can be specified in these forms: - \\- (literal minus character) - identifies the HEAD of the default branch. - name - Identifies the HEAD commit of a branch or tag. - name@hash - Identifies the 'hash' commit on a branch or tag. - @hash - Identifies the 'hash' commit in an unspecified branch or tag. - -~3 - The 3rd predecessor commit from the HEAD of the default branch. - name~3 - The 3rd predecessor commit from the HEAD of a branch or tag. - @hash~3 - The 3rd predecessor commit of the 'hash' commit. - name@hash^2 - The merge parent of the 'hash' commit of a branch or tag. - @hash^2 - The merge parent of the 'hash' commit. - -*2021-04-07T14:42:25.534748Z - The predecessor commit closest to the HEAD of the default branch for the given ISO-8601 timestamp. - name*2021-04-07T14:42:25.534748Z - The predecessor commit closest to the HEAD of a branch or tag valid for the given ISO-8601 timestamp. - name*1685185847230 - The predecessor commit closest to the HEAD of a branch or tag valid for the given timestamp in milliseconds since epoch.  If both 'name' and 'hash' are given, 'hash' must be reachable from the current HEAD of the branch or tag. If 'name' is omitted, the reference will be of type 'DETACHED' (referencing a specific commit hash without claiming its reachability from any live HEAD). Using references of the last form may have authorization implications when compared to an equivalent reference of the former forms.  An empty reference parameter is invalid.  The 'name@hash' form always refers to the exact commit on a specific named reference. This is the most complete form of a reference. Other forms omit some of the details and require those gaps to be filled by the server at runtime. Although these forms may be convenient to a human-being, they may resolve differently at different times depending on the state of the system. Using the full 'name@hash' form is recommended to avoid ambiguity. 
	content := true // bool | Optionally request to return 'Content' objects for the returned keys. (optional)
	filter := "filter_example" // string | A Common Expression Language (CEL) expression. An intro to CEL can be found at https://github.com/google/cel-spec/blob/master/doc/intro.md. Usable variables within the expression are 'entry.namespace' (string) & 'entry.contentType' (string) (optional)
	key := []openapiclient.ContentKey{*openapiclient.NewContentKey([]string{"Elements_example"})} // []ContentKey | Restrict the result to one or more keys.  Can be combined with min/max-key and prefix-key parameters, however both predicates must match. This means that keys specified via this parameter that do not match a given min/max-key or prefix-key will not be returned.  Content key and namespace components are separated by the dot (`.`) character. The components itself must be escaped using the rules described in [NESSIE-SPEC-2.0.md in the repository](https://github.com/projectnessie/nessie/blob/main/api/NESSIE-SPEC-2-0.md). (optional)
	maxKey := *openapiclient.NewContentKey([]string{"Elements_example"}) // ContentKey | The upper bound of the content key range to retrieve (inclusive). The content keys of all returned entries will be less than or equal to the max-value. Content-keys are compared as a 'whole', unlike prefix-keys.  Content key and namespace components are separated by the dot (`.`) character. The components itself must be escaped using the rules described in [NESSIE-SPEC-2.0.md in the repository](https://github.com/projectnessie/nessie/blob/main/api/NESSIE-SPEC-2-0.md). (optional)
	maxRecords := int32(56) // int32 | maximum number of entries to return, just a hint for the server (optional)
	minKey :=  // ContentKey | The lower bound of the content key range to retrieve (inclusive). The content keys of all returned entries will be greater than or equal to the min-value. Content-keys are compared as a 'whole', unlike prefix-keys.  Content key and namespace components are separated by the dot (`.`) character. The components itself must be escaped using the rules described in [NESSIE-SPEC-2.0.md in the repository](https://github.com/projectnessie/nessie/blob/main/api/NESSIE-SPEC-2-0.md). (optional)
	pageToken := "pageToken_example" // string | paging continuation token, as returned in the previous value of the field 'token' in the corresponding 'EntriesResponse' or 'LogResponse' or 'ReferencesResponse' or 'RefLogResponse'. (optional)
	prefixKey :=  // ContentKey | The content key prefix to retrieve (inclusive). A content key matches a given prefix, a content key's elements starts with all elements of the prefix-key. Key prefixes exactly match key-element boundaries.  Must not be combined with min/max-key parameters.  Content key and namespace components are separated by the dot (`.`) character. The components itself must be escaped using the rules described in [NESSIE-SPEC-2.0.md in the repository](https://github.com/projectnessie/nessie/blob/main/api/NESSIE-SPEC-2-0.md). (optional)

	configuration := openapiclient.NewConfiguration()
	apiClient := openapiclient.NewAPIClient(configuration)
	resp, r, err := apiClient.V2API.GetEntriesV2(context.Background(), ref).Content(content).Filter(filter).Key(key).MaxKey(maxKey).MaxRecords(maxRecords).MinKey(minKey).PageToken(pageToken).PrefixKey(prefixKey).Execute()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error when calling `V2API.GetEntriesV2``: %v\n", err)
		fmt.Fprintf(os.Stderr, "Full HTTP response: %v\n", r)
	}
	// response from `GetEntriesV2`: EntriesResponseV2
	fmt.Fprintf(os.Stdout, "Response from `V2API.GetEntriesV2`: %v\n", resp)
}
```

### Path Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
**ctx** | **context.Context** | context for authentication, logging, cancellation, deadlines, tracing, etc.
**ref** | [**interface{}**](.md) | A reference to a particular version of the contents tree (a point in history).  Reference representations consist of: - The reference name. &#39;-&#39; means the default branch name. - A commit hash prefixed with &#39;@&#39;. - A relative commit specification. &#39;~N&#39; means the N-th predecessor commit, &#39;*T&#39; means the commit for which the timestamp T (milliseconds since epoch or ISO-8601 instant) is valid, &#39;^N&#39; means the N-th parentin a commit (N&#x3D;2 is the merge parent).  If neither the reference name or the default branch name placeholder &#39;-&#39; is specified, the reference type &#39;DETACHED&#39; will be assumed. If no commit hash is specified, the HEAD of the specified named reference will be used. An empty reference parameter is not valid.  This reference can be specified in these forms: - \\- (literal minus character) - identifies the HEAD of the default branch. - name - Identifies the HEAD commit of a branch or tag. - name@hash - Identifies the &#39;hash&#39; commit on a branch or tag. - @hash - Identifies the &#39;hash&#39; commit in an unspecified branch or tag. - -~3 - The 3rd predecessor commit from the HEAD of the default branch. - name~3 - The 3rd predecessor commit from the HEAD of a branch or tag. - @hash~3 - The 3rd predecessor commit of the &#39;hash&#39; commit. - name@hash^2 - The merge parent of the &#39;hash&#39; commit of a branch or tag. - @hash^2 - The merge parent of the &#39;hash&#39; commit. - -*2021-04-07T14:42:25.534748Z - The predecessor commit closest to the HEAD of the default branch for the given ISO-8601 timestamp. - name*2021-04-07T14:42:25.534748Z - The predecessor commit closest to the HEAD of a branch or tag valid for the given ISO-8601 timestamp. - name*1685185847230 - The predecessor commit closest to the HEAD of a branch or tag valid for the given timestamp in milliseconds since epoch.  If both &#39;name&#39; and &#39;hash&#39; are given, &#39;hash&#39; must be reachable from the current HEAD of the branch or tag. If &#39;name&#39; is omitted, the reference will be of type &#39;DETACHED&#39; (referencing a specific commit hash without claiming its reachability from any live HEAD). Using references of the last form may have authorization implications when compared to an equivalent reference of the former forms.  An empty reference parameter is invalid.  The &#39;name@hash&#39; form always refers to the exact commit on a specific named reference. This is the most complete form of a reference. Other forms omit some of the details and require those gaps to be filled by the server at runtime. Although these forms may be convenient to a human-being, they may resolve differently at different times depending on the state of the system. Using the full &#39;name@hash&#39; form is recommended to avoid ambiguity.  | 

### Other Parameters

Other parameters are passed through a pointer to a apiGetEntriesV2Request struct via the builder pattern


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------

 **content** | **bool** | Optionally request to return &#39;Content&#39; objects for the returned keys. | 
 **filter** | **string** | A Common Expression Language (CEL) expression. An intro to CEL can be found at https://github.com/google/cel-spec/blob/master/doc/intro.md. Usable variables within the expression are &#39;entry.namespace&#39; (string) &amp; &#39;entry.contentType&#39; (string) | 
 **key** | [**[]ContentKey**](ContentKey.md) | Restrict the result to one or more keys.  Can be combined with min/max-key and prefix-key parameters, however both predicates must match. This means that keys specified via this parameter that do not match a given min/max-key or prefix-key will not be returned.  Content key and namespace components are separated by the dot (&#x60;.&#x60;) character. The components itself must be escaped using the rules described in [NESSIE-SPEC-2.0.md in the repository](https://github.com/projectnessie/nessie/blob/main/api/NESSIE-SPEC-2-0.md). | 
 **maxKey** | [**ContentKey**](ContentKey.md) | The upper bound of the content key range to retrieve (inclusive). The content keys of all returned entries will be less than or equal to the max-value. Content-keys are compared as a &#39;whole&#39;, unlike prefix-keys.  Content key and namespace components are separated by the dot (&#x60;.&#x60;) character. The components itself must be escaped using the rules described in [NESSIE-SPEC-2.0.md in the repository](https://github.com/projectnessie/nessie/blob/main/api/NESSIE-SPEC-2-0.md). | 
 **maxRecords** | **int32** | maximum number of entries to return, just a hint for the server | 
 **minKey** | [**ContentKey**](ContentKey.md) | The lower bound of the content key range to retrieve (inclusive). The content keys of all returned entries will be greater than or equal to the min-value. Content-keys are compared as a &#39;whole&#39;, unlike prefix-keys.  Content key and namespace components are separated by the dot (&#x60;.&#x60;) character. The components itself must be escaped using the rules described in [NESSIE-SPEC-2.0.md in the repository](https://github.com/projectnessie/nessie/blob/main/api/NESSIE-SPEC-2-0.md). | 
 **pageToken** | **string** | paging continuation token, as returned in the previous value of the field &#39;token&#39; in the corresponding &#39;EntriesResponse&#39; or &#39;LogResponse&#39; or &#39;ReferencesResponse&#39; or &#39;RefLogResponse&#39;. | 
 **prefixKey** | [**ContentKey**](ContentKey.md) | The content key prefix to retrieve (inclusive). A content key matches a given prefix, a content key&#39;s elements starts with all elements of the prefix-key. Key prefixes exactly match key-element boundaries.  Must not be combined with min/max-key parameters.  Content key and namespace components are separated by the dot (&#x60;.&#x60;) character. The components itself must be escaped using the rules described in [NESSIE-SPEC-2.0.md in the repository](https://github.com/projectnessie/nessie/blob/main/api/NESSIE-SPEC-2-0.md). | 

### Return type

[**EntriesResponseV2**](EntriesResponseV2.md)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints)
[[Back to Model list]](../README.md#documentation-for-models)
[[Back to README]](../README.md)


## GetMultipleContentsV2

> GetMultipleContentsResponseV2 GetMultipleContentsV2(ctx, ref).GetMultipleContentsRequest(getMultipleContentsRequest).ForWrite(forWrite).WithDoc(withDoc).Execute()

Get multiple content objects.



### Example

```go
package main

import (
	"context"
	"fmt"
	"os"
	openapiclient "github.com/GIT_USER_ID/GIT_REPO_ID"
)

func main() {
	ref := TODO // interface{} | A reference to a particular version of the contents tree (a point in history).  Reference representations consist of: - The reference name. '-' means the default branch name. - A commit hash prefixed with '@'. - A relative commit specification. '~N' means the N-th predecessor commit, '*T' means the commit for which the timestamp T (milliseconds since epoch or ISO-8601 instant) is valid, '^N' means the N-th parentin a commit (N=2 is the merge parent).  If neither the reference name or the default branch name placeholder '-' is specified, the reference type 'DETACHED' will be assumed. If no commit hash is specified, the HEAD of the specified named reference will be used. An empty reference parameter is not valid.  This reference can be specified in these forms: - \\- (literal minus character) - identifies the HEAD of the default branch. - name - Identifies the HEAD commit of a branch or tag. - name@hash - Identifies the 'hash' commit on a branch or tag. - @hash - Identifies the 'hash' commit in an unspecified branch or tag. - -~3 - The 3rd predecessor commit from the HEAD of the default branch. - name~3 - The 3rd predecessor commit from the HEAD of a branch or tag. - @hash~3 - The 3rd predecessor commit of the 'hash' commit. - name@hash^2 - The merge parent of the 'hash' commit of a branch or tag. - @hash^2 - The merge parent of the 'hash' commit. - -*2021-04-07T14:42:25.534748Z - The predecessor commit closest to the HEAD of the default branch for the given ISO-8601 timestamp. - name*2021-04-07T14:42:25.534748Z - The predecessor commit closest to the HEAD of a branch or tag valid for the given ISO-8601 timestamp. - name*1685185847230 - The predecessor commit closest to the HEAD of a branch or tag valid for the given timestamp in milliseconds since epoch.  If both 'name' and 'hash' are given, 'hash' must be reachable from the current HEAD of the branch or tag. If 'name' is omitted, the reference will be of type 'DETACHED' (referencing a specific commit hash without claiming its reachability from any live HEAD). Using references of the last form may have authorization implications when compared to an equivalent reference of the former forms.  An empty reference parameter is invalid.  The 'name@hash' form always refers to the exact commit on a specific named reference. This is the most complete form of a reference. Other forms omit some of the details and require those gaps to be filled by the server at runtime. Although these forms may be convenient to a human-being, they may resolve differently at different times depending on the state of the system. Using the full 'name@hash' form is recommended to avoid ambiguity. 
	getMultipleContentsRequest := *openapiclient.NewGetMultipleContentsRequest([]openapiclient.ContentKey{*openapiclient.NewContentKey([]string{"Elements_example"})}) // GetMultipleContentsRequest | Keys to retrieve.
	forWrite := true // bool | If set to 'true', access control checks will check for write/create privilege in addition to read privileges. (optional)
	withDoc := true // bool | Whether to return the documentation, if it exists. Default is to not return the documentation. (optional)

	configuration := openapiclient.NewConfiguration()
	apiClient := openapiclient.NewAPIClient(configuration)
	resp, r, err := apiClient.V2API.GetMultipleContentsV2(context.Background(), ref).GetMultipleContentsRequest(getMultipleContentsRequest).ForWrite(forWrite).WithDoc(withDoc).Execute()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error when calling `V2API.GetMultipleContentsV2``: %v\n", err)
		fmt.Fprintf(os.Stderr, "Full HTTP response: %v\n", r)
	}
	// response from `GetMultipleContentsV2`: GetMultipleContentsResponseV2
	fmt.Fprintf(os.Stdout, "Response from `V2API.GetMultipleContentsV2`: %v\n", resp)
}
```

### Path Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
**ctx** | **context.Context** | context for authentication, logging, cancellation, deadlines, tracing, etc.
**ref** | [**interface{}**](.md) | A reference to a particular version of the contents tree (a point in history).  Reference representations consist of: - The reference name. &#39;-&#39; means the default branch name. - A commit hash prefixed with &#39;@&#39;. - A relative commit specification. &#39;~N&#39; means the N-th predecessor commit, &#39;*T&#39; means the commit for which the timestamp T (milliseconds since epoch or ISO-8601 instant) is valid, &#39;^N&#39; means the N-th parentin a commit (N&#x3D;2 is the merge parent).  If neither the reference name or the default branch name placeholder &#39;-&#39; is specified, the reference type &#39;DETACHED&#39; will be assumed. If no commit hash is specified, the HEAD of the specified named reference will be used. An empty reference parameter is not valid.  This reference can be specified in these forms: - \\- (literal minus character) - identifies the HEAD of the default branch. - name - Identifies the HEAD commit of a branch or tag. - name@hash - Identifies the &#39;hash&#39; commit on a branch or tag. - @hash - Identifies the &#39;hash&#39; commit in an unspecified branch or tag. - -~3 - The 3rd predecessor commit from the HEAD of the default branch. - name~3 - The 3rd predecessor commit from the HEAD of a branch or tag. - @hash~3 - The 3rd predecessor commit of the &#39;hash&#39; commit. - name@hash^2 - The merge parent of the &#39;hash&#39; commit of a branch or tag. - @hash^2 - The merge parent of the &#39;hash&#39; commit. - -*2021-04-07T14:42:25.534748Z - The predecessor commit closest to the HEAD of the default branch for the given ISO-8601 timestamp. - name*2021-04-07T14:42:25.534748Z - The predecessor commit closest to the HEAD of a branch or tag valid for the given ISO-8601 timestamp. - name*1685185847230 - The predecessor commit closest to the HEAD of a branch or tag valid for the given timestamp in milliseconds since epoch.  If both &#39;name&#39; and &#39;hash&#39; are given, &#39;hash&#39; must be reachable from the current HEAD of the branch or tag. If &#39;name&#39; is omitted, the reference will be of type &#39;DETACHED&#39; (referencing a specific commit hash without claiming its reachability from any live HEAD). Using references of the last form may have authorization implications when compared to an equivalent reference of the former forms.  An empty reference parameter is invalid.  The &#39;name@hash&#39; form always refers to the exact commit on a specific named reference. This is the most complete form of a reference. Other forms omit some of the details and require those gaps to be filled by the server at runtime. Although these forms may be convenient to a human-being, they may resolve differently at different times depending on the state of the system. Using the full &#39;name@hash&#39; form is recommended to avoid ambiguity.  | 

### Other Parameters

Other parameters are passed through a pointer to a apiGetMultipleContentsV2Request struct via the builder pattern


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------

 **getMultipleContentsRequest** | [**GetMultipleContentsRequest**](GetMultipleContentsRequest.md) | Keys to retrieve. | 
 **forWrite** | **bool** | If set to &#39;true&#39;, access control checks will check for write/create privilege in addition to read privileges. | 
 **withDoc** | **bool** | Whether to return the documentation, if it exists. Default is to not return the documentation. | 

### Return type

[**GetMultipleContentsResponseV2**](GetMultipleContentsResponseV2.md)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints)
[[Back to Model list]](../README.md#documentation-for-models)
[[Back to README]](../README.md)


## GetReferenceByNameV2

> SingleReferenceResponseV2 GetReferenceByNameV2(ctx, ref).Fetch(fetch).Execute()

Fetch details of a reference

### Example

```go
package main

import (
	"context"
	"fmt"
	"os"
	openapiclient "github.com/GIT_USER_ID/GIT_REPO_ID"
)

func main() {
	ref := "ref_example" // string | Specifies a reference to a particular commit history branch or tag.  This reference can be specification in these forms: - \\- (literal minus character) - identifies the default branch. - name - Identifies the named branch or tag. 
	fetch := openapiclient.FetchOption("MINIMAL") // FetchOption | Specify how much information to be returned. Will fetch additional metadata for references if set to 'ALL'.  A returned Branch instance will have the following information:  - numCommitsAhead (number of commits ahead of the default branch)  - numCommitsBehind (number of commits behind the default branch)  - commitMetaOfHEAD (the commit metadata of the HEAD commit)  - commonAncestorHash (the hash of the common ancestor in relation to the default branch).  - numTotalCommits (the total number of commits in this reference).  A returned Tag instance will only contain the 'commitMetaOfHEAD' and 'numTotalCommits' fields.  Note that computing & fetching additional metadata might be computationally expensive on the server-side, so this flag should be used with care. (optional)

	configuration := openapiclient.NewConfiguration()
	apiClient := openapiclient.NewAPIClient(configuration)
	resp, r, err := apiClient.V2API.GetReferenceByNameV2(context.Background(), ref).Fetch(fetch).Execute()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error when calling `V2API.GetReferenceByNameV2``: %v\n", err)
		fmt.Fprintf(os.Stderr, "Full HTTP response: %v\n", r)
	}
	// response from `GetReferenceByNameV2`: SingleReferenceResponseV2
	fmt.Fprintf(os.Stdout, "Response from `V2API.GetReferenceByNameV2`: %v\n", resp)
}
```

### Path Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
**ctx** | **context.Context** | context for authentication, logging, cancellation, deadlines, tracing, etc.
**ref** | **string** | Specifies a reference to a particular commit history branch or tag.  This reference can be specification in these forms: - \\- (literal minus character) - identifies the default branch. - name - Identifies the named branch or tag.  | 

### Other Parameters

Other parameters are passed through a pointer to a apiGetReferenceByNameV2Request struct via the builder pattern


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------

 **fetch** | [**FetchOption**](FetchOption.md) | Specify how much information to be returned. Will fetch additional metadata for references if set to &#39;ALL&#39;.  A returned Branch instance will have the following information:  - numCommitsAhead (number of commits ahead of the default branch)  - numCommitsBehind (number of commits behind the default branch)  - commitMetaOfHEAD (the commit metadata of the HEAD commit)  - commonAncestorHash (the hash of the common ancestor in relation to the default branch).  - numTotalCommits (the total number of commits in this reference).  A returned Tag instance will only contain the &#39;commitMetaOfHEAD&#39; and &#39;numTotalCommits&#39; fields.  Note that computing &amp; fetching additional metadata might be computationally expensive on the server-side, so this flag should be used with care. | 

### Return type

[**SingleReferenceResponseV2**](SingleReferenceResponseV2.md)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints)
[[Back to Model list]](../README.md#documentation-for-models)
[[Back to README]](../README.md)


## GetReferenceHistory

> ReferenceHistoryResponseV2 GetReferenceHistory(ctx, ref).ScanCommits(scanCommits).Execute()

Fetch recent pointer changes of a reference



### Example

```go
package main

import (
	"context"
	"fmt"
	"os"
	openapiclient "github.com/GIT_USER_ID/GIT_REPO_ID"
)

func main() {
	ref := "ref_example" // string | Specifies a reference to a particular commit history branch or tag.  This reference can be specification in these forms: - \\- (literal minus character) - identifies the default branch. - name - Identifies the named branch or tag. 
	scanCommits := int32(56) // int32 | Optional parameter, specifies the number of commits to scan from the reference's current HEAD, limited to the given amount of commits. Default is to not scan the commit log. The server may impose a hard limit on the amount of commits from the commit log. (optional)

	configuration := openapiclient.NewConfiguration()
	apiClient := openapiclient.NewAPIClient(configuration)
	resp, r, err := apiClient.V2API.GetReferenceHistory(context.Background(), ref).ScanCommits(scanCommits).Execute()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error when calling `V2API.GetReferenceHistory``: %v\n", err)
		fmt.Fprintf(os.Stderr, "Full HTTP response: %v\n", r)
	}
	// response from `GetReferenceHistory`: ReferenceHistoryResponseV2
	fmt.Fprintf(os.Stdout, "Response from `V2API.GetReferenceHistory`: %v\n", resp)
}
```

### Path Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
**ctx** | **context.Context** | context for authentication, logging, cancellation, deadlines, tracing, etc.
**ref** | **string** | Specifies a reference to a particular commit history branch or tag.  This reference can be specification in these forms: - \\- (literal minus character) - identifies the default branch. - name - Identifies the named branch or tag.  | 

### Other Parameters

Other parameters are passed through a pointer to a apiGetReferenceHistoryRequest struct via the builder pattern


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------

 **scanCommits** | **int32** | Optional parameter, specifies the number of commits to scan from the reference&#39;s current HEAD, limited to the given amount of commits. Default is to not scan the commit log. The server may impose a hard limit on the amount of commits from the commit log. | 

### Return type

[**ReferenceHistoryResponseV2**](ReferenceHistoryResponseV2.md)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints)
[[Back to Model list]](../README.md#documentation-for-models)
[[Back to README]](../README.md)


## GetRepositoryConfig

> RepositoryConfigurationObjectsForTheRequestedTypes GetRepositoryConfig(ctx).Type_(type_).Execute()

Returns repository configurations of the requested types.

### Example

```go
package main

import (
	"context"
	"fmt"
	"os"
	openapiclient "github.com/GIT_USER_ID/GIT_REPO_ID"
)

func main() {
	type_ := []string{"Inner_example"} // []string |  (optional)

	configuration := openapiclient.NewConfiguration()
	apiClient := openapiclient.NewAPIClient(configuration)
	resp, r, err := apiClient.V2API.GetRepositoryConfig(context.Background()).Type_(type_).Execute()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error when calling `V2API.GetRepositoryConfig``: %v\n", err)
		fmt.Fprintf(os.Stderr, "Full HTTP response: %v\n", r)
	}
	// response from `GetRepositoryConfig`: RepositoryConfigurationObjectsForTheRequestedTypes
	fmt.Fprintf(os.Stdout, "Response from `V2API.GetRepositoryConfig`: %v\n", resp)
}
```

### Path Parameters



### Other Parameters

Other parameters are passed through a pointer to a apiGetRepositoryConfigRequest struct via the builder pattern


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **type_** | **[]string** |  | 

### Return type

[**RepositoryConfigurationObjectsForTheRequestedTypes**](RepositoryConfigurationObjectsForTheRequestedTypes.md)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints)
[[Back to Model list]](../README.md#documentation-for-models)
[[Back to README]](../README.md)


## GetSeveralContents

> GetMultipleContentsResponseV2 GetSeveralContents(ctx, ref).ForWrite(forWrite).Key(key).WithDoc(withDoc).Execute()

Get multiple content objects.



### Example

```go
package main

import (
	"context"
	"fmt"
	"os"
	openapiclient "github.com/GIT_USER_ID/GIT_REPO_ID"
)

func main() {
	ref := TODO // interface{} | A reference to a particular version of the contents tree (a point in history).  Reference representations consist of: - The reference name. '-' means the default branch name. - A commit hash prefixed with '@'. - A relative commit specification. '~N' means the N-th predecessor commit, '*T' means the commit for which the timestamp T (milliseconds since epoch or ISO-8601 instant) is valid, '^N' means the N-th parentin a commit (N=2 is the merge parent).  If neither the reference name or the default branch name placeholder '-' is specified, the reference type 'DETACHED' will be assumed. If no commit hash is specified, the HEAD of the specified named reference will be used. An empty reference parameter is not valid.  This reference can be specified in these forms: - \\- (literal minus character) - identifies the HEAD of the default branch. - name - Identifies the HEAD commit of a branch or tag. - name@hash - Identifies the 'hash' commit on a branch or tag. - @hash - Identifies the 'hash' commit in an unspecified branch or tag. - -~3 - The 3rd predecessor commit from the HEAD of the default branch. - name~3 - The 3rd predecessor commit from the HEAD of a branch or tag. - @hash~3 - The 3rd predecessor commit of the 'hash' commit. - name@hash^2 - The merge parent of the 'hash' commit of a branch or tag. - @hash^2 - The merge parent of the 'hash' commit. - -*2021-04-07T14:42:25.534748Z - The predecessor commit closest to the HEAD of the default branch for the given ISO-8601 timestamp. - name*2021-04-07T14:42:25.534748Z - The predecessor commit closest to the HEAD of a branch or tag valid for the given ISO-8601 timestamp. - name*1685185847230 - The predecessor commit closest to the HEAD of a branch or tag valid for the given timestamp in milliseconds since epoch.  If both 'name' and 'hash' are given, 'hash' must be reachable from the current HEAD of the branch or tag. If 'name' is omitted, the reference will be of type 'DETACHED' (referencing a specific commit hash without claiming its reachability from any live HEAD). Using references of the last form may have authorization implications when compared to an equivalent reference of the former forms.  An empty reference parameter is invalid.  The 'name@hash' form always refers to the exact commit on a specific named reference. This is the most complete form of a reference. Other forms omit some of the details and require those gaps to be filled by the server at runtime. Although these forms may be convenient to a human-being, they may resolve differently at different times depending on the state of the system. Using the full 'name@hash' form is recommended to avoid ambiguity. 
	forWrite := true // bool | If set to 'true', access control checks will check for write/create privilege in addition to read privileges. (optional)
	key := []string{"Inner_example"} // []string | The key to a content object.  Content key and namespace components are separated by the dot (`.`) character. The components itself must be escaped using the rules described in [NESSIE-SPEC-2.0.md in the repository](https://github.com/projectnessie/nessie/blob/main/api/NESSIE-SPEC-2-0.md). (optional)
	withDoc := true // bool | Whether to return the documentation, if it exists. Default is to not return the documentation. (optional)

	configuration := openapiclient.NewConfiguration()
	apiClient := openapiclient.NewAPIClient(configuration)
	resp, r, err := apiClient.V2API.GetSeveralContents(context.Background(), ref).ForWrite(forWrite).Key(key).WithDoc(withDoc).Execute()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error when calling `V2API.GetSeveralContents``: %v\n", err)
		fmt.Fprintf(os.Stderr, "Full HTTP response: %v\n", r)
	}
	// response from `GetSeveralContents`: GetMultipleContentsResponseV2
	fmt.Fprintf(os.Stdout, "Response from `V2API.GetSeveralContents`: %v\n", resp)
}
```

### Path Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
**ctx** | **context.Context** | context for authentication, logging, cancellation, deadlines, tracing, etc.
**ref** | [**interface{}**](.md) | A reference to a particular version of the contents tree (a point in history).  Reference representations consist of: - The reference name. &#39;-&#39; means the default branch name. - A commit hash prefixed with &#39;@&#39;. - A relative commit specification. &#39;~N&#39; means the N-th predecessor commit, &#39;*T&#39; means the commit for which the timestamp T (milliseconds since epoch or ISO-8601 instant) is valid, &#39;^N&#39; means the N-th parentin a commit (N&#x3D;2 is the merge parent).  If neither the reference name or the default branch name placeholder &#39;-&#39; is specified, the reference type &#39;DETACHED&#39; will be assumed. If no commit hash is specified, the HEAD of the specified named reference will be used. An empty reference parameter is not valid.  This reference can be specified in these forms: - \\- (literal minus character) - identifies the HEAD of the default branch. - name - Identifies the HEAD commit of a branch or tag. - name@hash - Identifies the &#39;hash&#39; commit on a branch or tag. - @hash - Identifies the &#39;hash&#39; commit in an unspecified branch or tag. - -~3 - The 3rd predecessor commit from the HEAD of the default branch. - name~3 - The 3rd predecessor commit from the HEAD of a branch or tag. - @hash~3 - The 3rd predecessor commit of the &#39;hash&#39; commit. - name@hash^2 - The merge parent of the &#39;hash&#39; commit of a branch or tag. - @hash^2 - The merge parent of the &#39;hash&#39; commit. - -*2021-04-07T14:42:25.534748Z - The predecessor commit closest to the HEAD of the default branch for the given ISO-8601 timestamp. - name*2021-04-07T14:42:25.534748Z - The predecessor commit closest to the HEAD of a branch or tag valid for the given ISO-8601 timestamp. - name*1685185847230 - The predecessor commit closest to the HEAD of a branch or tag valid for the given timestamp in milliseconds since epoch.  If both &#39;name&#39; and &#39;hash&#39; are given, &#39;hash&#39; must be reachable from the current HEAD of the branch or tag. If &#39;name&#39; is omitted, the reference will be of type &#39;DETACHED&#39; (referencing a specific commit hash without claiming its reachability from any live HEAD). Using references of the last form may have authorization implications when compared to an equivalent reference of the former forms.  An empty reference parameter is invalid.  The &#39;name@hash&#39; form always refers to the exact commit on a specific named reference. This is the most complete form of a reference. Other forms omit some of the details and require those gaps to be filled by the server at runtime. Although these forms may be convenient to a human-being, they may resolve differently at different times depending on the state of the system. Using the full &#39;name@hash&#39; form is recommended to avoid ambiguity.  | 

### Other Parameters

Other parameters are passed through a pointer to a apiGetSeveralContentsRequest struct via the builder pattern


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------

 **forWrite** | **bool** | If set to &#39;true&#39;, access control checks will check for write/create privilege in addition to read privileges. | 
 **key** | **[]string** | The key to a content object.  Content key and namespace components are separated by the dot (&#x60;.&#x60;) character. The components itself must be escaped using the rules described in [NESSIE-SPEC-2.0.md in the repository](https://github.com/projectnessie/nessie/blob/main/api/NESSIE-SPEC-2-0.md). | 
 **withDoc** | **bool** | Whether to return the documentation, if it exists. Default is to not return the documentation. | 

### Return type

[**GetMultipleContentsResponseV2**](GetMultipleContentsResponseV2.md)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints)
[[Back to Model list]](../README.md#documentation-for-models)
[[Back to README]](../README.md)


## MergeV2

> MergeResponseV2 MergeV2(ctx, branch).Merge1(merge1).Execute()

Merge commits from another reference onto 'branch'.



### Example

```go
package main

import (
	"context"
	"fmt"
	"os"
	openapiclient "github.com/GIT_USER_ID/GIT_REPO_ID"
)

func main() {
	branch := TODO // interface{} | A reference to a specific version of the contents tree (a point in history) on a branch. This reference is specified in this form: - name@hash - Identifies the 'hash' commit on the named branch.  The 'hash' commit must be reachable from the current HEAD of the branch. In this case 'hash' indicates the state of contents known to the client and serves to ensure that the operation is performed on the contents that the client expects. This hash can point to a commit in the middle of the change history, but it should be as recent as possible. 
	merge1 := *openapiclient.NewMerge1("FromRefName_example") // Merge1 | Merge operation that defines the source reference name and an optional hash. If 'fromHash' is not present, the current 'sourceRef's HEAD will be used.

	configuration := openapiclient.NewConfiguration()
	apiClient := openapiclient.NewAPIClient(configuration)
	resp, r, err := apiClient.V2API.MergeV2(context.Background(), branch).Merge1(merge1).Execute()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error when calling `V2API.MergeV2``: %v\n", err)
		fmt.Fprintf(os.Stderr, "Full HTTP response: %v\n", r)
	}
	// response from `MergeV2`: MergeResponseV2
	fmt.Fprintf(os.Stdout, "Response from `V2API.MergeV2`: %v\n", resp)
}
```

### Path Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
**ctx** | **context.Context** | context for authentication, logging, cancellation, deadlines, tracing, etc.
**branch** | [**interface{}**](.md) | A reference to a specific version of the contents tree (a point in history) on a branch. This reference is specified in this form: - name@hash - Identifies the &#39;hash&#39; commit on the named branch.  The &#39;hash&#39; commit must be reachable from the current HEAD of the branch. In this case &#39;hash&#39; indicates the state of contents known to the client and serves to ensure that the operation is performed on the contents that the client expects. This hash can point to a commit in the middle of the change history, but it should be as recent as possible.  | 

### Other Parameters

Other parameters are passed through a pointer to a apiMergeV2Request struct via the builder pattern


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------

 **merge1** | [**Merge1**](Merge1.md) | Merge operation that defines the source reference name and an optional hash. If &#39;fromHash&#39; is not present, the current &#39;sourceRef&#39;s HEAD will be used. | 

### Return type

[**MergeResponseV2**](MergeResponseV2.md)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints)
[[Back to Model list]](../README.md#documentation-for-models)
[[Back to README]](../README.md)


## TransplantV2

> MergeResponseV2 TransplantV2(ctx, branch).Transplant1(transplant1).Execute()

Transplant commits specified by the 'Transplant' payload object onto the given 'branch'



### Example

```go
package main

import (
	"context"
	"fmt"
	"os"
	openapiclient "github.com/GIT_USER_ID/GIT_REPO_ID"
)

func main() {
	branch := TODO // interface{} | A reference to a specific version of the contents tree (a point in history) on a branch. This reference is specified in this form: - name@hash - Identifies the 'hash' commit on the named branch.  The 'hash' commit must be reachable from the current HEAD of the branch. In this case 'hash' indicates the state of contents known to the client and serves to ensure that the operation is performed on the contents that the client expects. This hash can point to a commit in the middle of the change history, but it should be as recent as possible. 
	transplant1 := *openapiclient.NewTransplant1([]string{"HashesToTransplant_example"}, "FromRefName_example") // Transplant1 | Commits to transplant

	configuration := openapiclient.NewConfiguration()
	apiClient := openapiclient.NewAPIClient(configuration)
	resp, r, err := apiClient.V2API.TransplantV2(context.Background(), branch).Transplant1(transplant1).Execute()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error when calling `V2API.TransplantV2``: %v\n", err)
		fmt.Fprintf(os.Stderr, "Full HTTP response: %v\n", r)
	}
	// response from `TransplantV2`: MergeResponseV2
	fmt.Fprintf(os.Stdout, "Response from `V2API.TransplantV2`: %v\n", resp)
}
```

### Path Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
**ctx** | **context.Context** | context for authentication, logging, cancellation, deadlines, tracing, etc.
**branch** | [**interface{}**](.md) | A reference to a specific version of the contents tree (a point in history) on a branch. This reference is specified in this form: - name@hash - Identifies the &#39;hash&#39; commit on the named branch.  The &#39;hash&#39; commit must be reachable from the current HEAD of the branch. In this case &#39;hash&#39; indicates the state of contents known to the client and serves to ensure that the operation is performed on the contents that the client expects. This hash can point to a commit in the middle of the change history, but it should be as recent as possible.  | 

### Other Parameters

Other parameters are passed through a pointer to a apiTransplantV2Request struct via the builder pattern


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------

 **transplant1** | [**Transplant1**](Transplant1.md) | Commits to transplant | 

### Return type

[**MergeResponseV2**](MergeResponseV2.md)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints)
[[Back to Model list]](../README.md#documentation-for-models)
[[Back to README]](../README.md)


## UpdateRepositoryConfig

> ThePreviousStateOfTheRepositoryConfigurationObject UpdateRepositoryConfig(ctx).Execute()

Create or update a repository configuration.

### Example

```go
package main

import (
	"context"
	"fmt"
	"os"
	openapiclient "github.com/GIT_USER_ID/GIT_REPO_ID"
)

func main() {

	configuration := openapiclient.NewConfiguration()
	apiClient := openapiclient.NewAPIClient(configuration)
	resp, r, err := apiClient.V2API.UpdateRepositoryConfig(context.Background()).Execute()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error when calling `V2API.UpdateRepositoryConfig``: %v\n", err)
		fmt.Fprintf(os.Stderr, "Full HTTP response: %v\n", r)
	}
	// response from `UpdateRepositoryConfig`: ThePreviousStateOfTheRepositoryConfigurationObject
	fmt.Fprintf(os.Stdout, "Response from `V2API.UpdateRepositoryConfig`: %v\n", resp)
}
```

### Path Parameters

This endpoint does not need any parameter.

### Other Parameters

Other parameters are passed through a pointer to a apiUpdateRepositoryConfigRequest struct via the builder pattern


### Return type

[**ThePreviousStateOfTheRepositoryConfigurationObject**](ThePreviousStateOfTheRepositoryConfigurationObject.md)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints)
[[Back to Model list]](../README.md#documentation-for-models)
[[Back to README]](../README.md)

