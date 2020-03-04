package mods

import (
	"plugin"

	"github.com/journeymidnight/yig/helper"
)

/*YigPlugin is the exported variable from plugins.
* the Name here is defined in plugin's code. the plugin is opened when config file is set.
* the PluginType is for different interface type
* such as:
* IAM_PLUGIN => IamClient interface
* UNKNOWN_PLUGIN=> other interface
 */
type YigPlugin struct {
	Name       string
	PluginType int
	Create     func(map[string]interface{}) (interface{}, error)
}

const EXPORTED_PLUGIN = "Exported"

const (
	IAM_PLUGIN = iota //IamClient interface
	NUMS_PLUGIN
)

func InitialPlugins() map[string]*YigPlugin {

	globalPlugins := make(map[string]*YigPlugin)
	var sopath string

	for name, pluginConfig := range helper.CONFIG.Plugins {
		sopath = pluginConfig.Path
		helper.Logger.Info(nil, "plugins: open for", name)
		if pluginConfig.Path == "" {
			helper.Logger.Error(nil, "plugin path for is empty", name)
			continue
		}

		//if enable do not exist in toml file, enable's default is false
		if pluginConfig.Enable == false {
			helper.Logger.Error(nil, "plugins: is not enabled, continue", sopath)
			continue
		}

		//open plugin file
		plug, err := plugin.Open(sopath)
		if err != nil {
			helper.Logger.Error(nil, "plugins: failed to open", sopath, "for", name)
			continue
		}
		exported, err := plug.Lookup(EXPORTED_PLUGIN)
		if err != nil {
			helper.Logger.Error(nil, "plugins: lookup failed, err: ", EXPORTED_PLUGIN, sopath, err)
			continue
		}

		//check plugin type
		yigPlugin, ok := exported.(*YigPlugin)
		if !ok {
			helper.Logger.Error(nil, "plugins: convert failed, exported:\n", EXPORTED_PLUGIN, sopath, exported)
			continue
		}

		//check plugin content
		if yigPlugin.Name == name && yigPlugin.Create != nil {
			globalPlugins[yigPlugin.Name] = yigPlugin
		} else {
			helper.Logger.Error(nil, "plugins: check failed, value:", sopath, yigPlugin)
			continue
		}
		helper.Logger.Info(nil, "plugins: loaded plugin from", yigPlugin.Name, sopath)
	}

	return globalPlugins
}
