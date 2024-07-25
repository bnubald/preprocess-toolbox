# BAS Download Toolbox

![GitHub issues](https://img.shields.io/github/issues/antarctica/preprocess-toolbox?style=plastic)
![GitHub closed issues](https://img.shields.io/github/issues-closed/antarctica/preprocess-toolbox?style=plastic)
![GitHub](https://img.shields.io/github/license/antarctica/preprocess-toolbox)
![GitHub forks](https://img.shields.io/github/forks/antarctica/preprocess-toolbox?style=social)
![GitHub forks](https://img.shields.io/github/stars/antarctica/preprocess-toolbox?style=social)

This is the preprocessing library for taking `download-toolbox` datasets and combining / composing multi-source data loaders that can be used to cache or supply downstream applications.

This is only just getting started, more info will appear soon.

Contact `jambyr <at> bas <dot> ac <dot> uk` if you want further information.

## Table of contents

* [Overview](#overview)
* [Installation](#installation)
* [Implementation](#implementation)
* [Contributing](#contributing)
* [Credits](#credits)
* [License](#license)

## Installation

Not currently released to pip.

Please refer to [the contribution guidelines for more information.](CONTRIBUTING.rst)

## Implementation

When installed, the library will provide a series of CLI commands. Please use 
the `--help` switch for more initial information, or the documentation. 

### Basic principles

The library provides the ability to preprocess `download-toolbox` datasets and create singular configurations for reading out the data in a multi-channel format for dataset construction:

...TODO...

This is a base library upon which application specific processing is based, lowering the implementation overhead for creating multi-source datasets for environmental applications that require integration of data from sources that `download-toolbox` provide. 

This library doesn't have knowledge of those datasets, it forms the basis for processing things specific to an application by importing application-specific logic dynamically. See [this issue](https://github.com/environmental-forecasting/preprocess-toolbox/issues/1) for a quick idea of how this works with the [IceNet workflow](https://github.com/icenet-ai/icenet).

## Limitations

There are some major limitations to this as a general purpose tool, these will hopefully be dealt with in time! I'm raising issues as I go

**This is currently very heavy development functionality, but the following commands already work**: 

* preprocess_missing_spatial - poorly at present due to missing mask backref implementation
* preprocess_missing_time
* preprocess_regrid
* preprocess_rotate
* preprocess_dataset
* preprocess_loader_init
* preprocess_add_mask
* preprocess_add_channel

Other stubs probably don't work, unless I forgot to update these docs!

## Contributing 

Please refer to [the contribution guidelines for more information.](CONTRIBUTING.rst)

## Credits

<a href="https://github.com/antarctica/preprocess-toolbox/graphs/contributors">
  <img src="https://contrib.rocks/image?repo=antarctica/preprocess-toolbox" />
</a>

## License

This is licensed using the [MIT License](LICENSE)
