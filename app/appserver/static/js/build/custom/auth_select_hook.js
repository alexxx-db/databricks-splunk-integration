class AuthSelectHook {
    constructor(globalConfig, serviceName, state, mode, util) {
        this.globalConfig = globalConfig;
        this.serviceName = serviceName;
        this.state = state;
        this.mode = mode;
        this.util = util;
    }

    onChange(field, value, dataDict) {
        if (field == 'auth_type') {
            this.toggleAuthFields(value);
        }
        if (field == 'config_for_dbquery') {
            if (value == 'interactive_cluster') {
                this.hideWarehouseField(false);
            } else {
                this.hideWarehouseField(true);
            }
        }
    }

    onRender() {
        var selected_auth = this.state.data.auth_type.value;
        this.toggleAuthFields(selected_auth);
    }

    hideWarehouseField(state) {
        this.util.setState((prevState) => {
            let data = {...prevState.data };
            data.warehouse_id.display = state;
            return { data }
        }); 
    }

    toggleAuthFields(authType) {
        this.util.setState((prevState) => {
            let data = {...prevState.data };

            // OAuth M2M fields
            const showOAuth = (authType === 'OAUTH_M2M');
            data.oauth_client_id.display = showOAuth;
            data.oauth_client_secret.display = showOAuth;

            // AAD fields
            const showAAD = (authType === 'AAD');
            data.aad_client_id.display = showAAD;
            data.aad_tenant_id.display = showAAD;
            data.aad_client_secret.display = showAAD;

            // PAT field
            const showPAT = (authType === 'PAT');
            data.databricks_pat.display = showPAT;

            return { data }
        });
    }

}

export default AuthSelectHook;