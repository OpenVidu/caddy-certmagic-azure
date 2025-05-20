package certmagic_azure

import (
	"context"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"strings"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/caddyserver/caddy/v2"
	"github.com/caddyserver/caddy/v2/caddyconfig/caddyfile"
	"github.com/caddyserver/certmagic"
	"go.uber.org/zap"
)

type AzureBlob struct {
	logger *zap.Logger

	// Azure Blob Storage
	AccountName string `json:"account_name"`
	AccountKey  string `json:"account_key"`
	Container   string `json:"container"`
	Prefix      string `json:"prefix"`
	Client      *azblob.Client
}

func init() {
	caddy.RegisterModule(AzureBlob{})
}

func (az *AzureBlob) UnmarshalCaddyfile(d *caddyfile.Dispenser) error {
	for d.Next() {
		var value string

		key := d.Val()

		if !d.Args(&value) {
			continue
		}

		switch key {
		case "account_name":
			az.AccountName = value
		case "account_key":
			az.AccountKey = value
		case "container":
			az.Container = value
		case "prefix":
			az.Prefix = value
		}
	}

	return nil
}

func (az *AzureBlob) Provision(ctx caddy.Context) error {
	az.logger = ctx.Logger(az)

	// Load Environment
	if az.AccountName == "" {
		az.AccountName = os.Getenv("AZURE_ACCOUNT_NAME")
	}

	if az.AccountKey == "" {
		az.AccountKey = os.Getenv("AZURE_ACCOUNT_KEY")
	}

	if az.Container == "" {
		az.Container = os.Getenv("AZURE_CONTAINER")
	}

	if az.Prefix == "" {
		az.Prefix = os.Getenv("AZURE_PREFIX")
	}

	// Create Azure Blob Storage client
	cred, err := azblob.NewSharedKeyCredential(az.AccountName, az.AccountKey)
	if err != nil {
		return fmt.Errorf("failed to create Azure credentials: %w", err)
	}

	client, err := azblob.NewClientWithSharedKeyCredential(fmt.Sprintf("https://%s.blob.core.windows.net", az.AccountName), cred, nil)
	if err != nil {
		return fmt.Errorf("failed to create Azure Blob client: %w", err)
	}

	az.Client = client
	return nil
}

func (AzureBlob) CaddyModule() caddy.ModuleInfo {
	return caddy.ModuleInfo{
		ID: "caddy.storage.azure_blob",
		New: func() caddy.Module {
			return new(AzureBlob)
		},
	}
}

func (az AzureBlob) CertMagicStorage() (certmagic.Storage, error) {
	return az, nil
}

func (az AzureBlob) Lock(ctx context.Context, key string) error {
	return nil
}

func (az AzureBlob) Unlock(ctx context.Context, key string) error {
	return nil
}

func (az AzureBlob) Store(ctx context.Context, key string, value []byte) error {
	key = az.KeyPrefix(key)
	az.logger.Debug(fmt.Sprintf("Store: %s, %d bytes", key, len(value)))

	_, err := az.Client.UploadBuffer(ctx, az.Container, key, value, nil)
	return err
}

func (az AzureBlob) Load(ctx context.Context, key string) ([]byte, error) {
	if !az.Exists(ctx, key) {
		return nil, fs.ErrNotExist
	}

	key = az.KeyPrefix(key)
	az.logger.Debug(fmt.Sprintf("Load key: %s", key))

	resp, err := az.Client.DownloadStream(ctx, az.Container, key, nil)
	if err != nil {
		return nil, err
	}

	return io.ReadAll(resp.Body)
}

func (az AzureBlob) Delete(ctx context.Context, key string) error {
	key = az.KeyPrefix(key)
	az.logger.Debug(fmt.Sprintf("Delete key: %s", key))

	_, err := az.Client.DeleteBlob(ctx, az.Container, key, nil)
	return err
}

func (az AzureBlob) Exists(ctx context.Context, key string) bool {
	// Create a service client
	serviceURL := fmt.Sprintf("https://%s.blob.core.windows.net/", az.AccountName)
	cred, err := azblob.NewSharedKeyCredential(az.AccountName, az.AccountKey)
	if err != nil {
		return false
	}

	azBlobClient, err := azblob.NewClientWithSharedKeyCredential(serviceURL, cred, nil)
	if err != nil {
		return false
	}

	// Get a client for the container
	containerClient := azBlobClient.ServiceClient().NewContainerClient(az.Container)

	// Get a client for the blob
	blobClient := containerClient.NewBlobClient(key)

	// Check if the blob exists
	_, err = blobClient.GetProperties(context.Background(), nil)

	if err != nil {
		return false // Some other error occurred
	}

	return true // Blob exists
}

func (az AzureBlob) List(ctx context.Context, prefix string, recursive bool) ([]string, error) {
	prefix = az.KeyPrefix(prefix)
	az.logger.Debug(fmt.Sprintf("List prefix: %s", prefix))

	pager := az.Client.NewListBlobsFlatPager(az.Container, &azblob.ListBlobsFlatOptions{
		Prefix: &prefix,
	})

	var keys []string
	for pager.More() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			return nil, err
		}

		for _, blob := range page.Segment.BlobItems {
			keys = append(keys, az.CutKeyPrefix(*blob.Name))
		}
	}

	return keys, nil
}

func (az AzureBlob) Stat(ctx context.Context, key string) (certmagic.KeyInfo, error) {
	key = az.KeyPrefix(key)

	props, err := az.Client.ServiceClient().NewContainerClient(az.Container).NewBlobClient(key).GetProperties(ctx, nil)
	if err != nil {
		az.logger.Error(fmt.Sprintf("Stat key: %s, error: %v", key, err))
		return certmagic.KeyInfo{}, nil
	}

	az.logger.Debug(fmt.Sprintf("Stat key: %s, size: %d bytes", key, *props.ContentLength))

	return certmagic.KeyInfo{
		Key:        key,
		Modified:   *props.LastModified,
		Size:       *props.ContentLength,
		IsTerminal: strings.HasSuffix(key, "/"),
	}, nil
}

func (az AzureBlob) KeyPrefix(key string) string {
	return path.Join(az.Prefix, key)
}

func (az AzureBlob) CutKeyPrefix(key string) string {
	cutted, _ := strings.CutPrefix(key, az.Prefix)
	return cutted
}

func (az AzureBlob) String() string {
	return fmt.Sprintf("Azure Blob Storage Account: %s, Container: %s, Prefix: %s", az.AccountName, az.Container, az.Prefix)
}
