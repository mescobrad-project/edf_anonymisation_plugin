from models.plugin import EmptyPlugin, PluginActionResponse, PluginExchangeMetadata

class GenericPlugin(EmptyPlugin):

    def action(self, input_meta: PluginExchangeMetadata = None) -> PluginActionResponse:
        
        # Load input data using internal method self.__load__(input_meta)
        # Use self.__MY_DEFINED_CONFIG__
        # Do something
        # Return a PluginActionResponse (Can be empty in case of action plugin)

        return PluginActionResponse(
            file_content_type="text/plain",
            file_content="some content"
        )
