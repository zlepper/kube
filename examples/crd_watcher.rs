use std::sync::Arc;
use anyhow::{Result};
use garde::Validate;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::time::Duration;
use tokio::time::sleep;
use tracing::*;

use k8s_openapi::apiextensions_apiserver::pkg::apis::apiextensions::v1::CustomResourceDefinition;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
use kube::runtime::watcher::Config;
use kube::{
    api::{Api, Patch, PatchParams, ResourceExt},
    core::crd::CustomResourceExt,
    Client, CustomResource,
};
use kube::runtime::Controller;
use kube::runtime::controller::Action;
use kube::runtime::reflector::ObjectRef;
use futures::{FutureExt, StreamExt};
use tokio::time;


#[derive(CustomResource, Deserialize, Serialize, Clone, Debug, Validate, JsonSchema)]
#[kube(group = "clux.dev", version = "v1", kind = "MainThing", namespaced)]
pub struct MainThingSpec {}

#[derive(CustomResource, Deserialize, Serialize, Clone, Debug, Validate, JsonSchema)]
#[kube(group = "clux.dev", version = "v1", kind = "RefererThing", namespaced)]
pub struct RefererThingSpec {
    #[garde(skip)]
    pub main_thing_name: String,
    #[garde(skip)]
    pub main_thing_namespace: Option<String>,
    #[garde(skip)]
    pub value: i32,
}


#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();


    ensure_crds().await?;


    for i in 0..100 {
        run_iteration(i).await?;
    }


    Ok(())
}

async fn run_iteration(large_iteration_index: i32) -> Result<()> {
    let client = Client::try_default().await?;

    let main_thing_api: Api<MainThing> = Api::all(client.clone());
    let referer_thing_api: Api<RefererThing> = Api::all(client.clone());

    let mut stream = Box::pin(Controller::new(main_thing_api.clone(), Config::default())
        .watches(referer_thing_api, Config::default(), |o| {
            let current_namespace = o.namespace().expect("referer thing should always be namespaced");
            let object_ref = ObjectRef::new(&o.spec.main_thing_name).within(o.spec.main_thing_namespace.as_deref().unwrap_or(&current_namespace));
            Some(object_ref)
        })
        .run(
            reconcile_main_thing,
            error_policy, Arc::new(())));

    let now = time::Instant::now();

    while now + Duration::from_secs(1) > time::Instant::now() {
        stream.next().now_or_never();
        sleep(Duration::from_millis(1)).await;
    }


    info!("Ready for large iteration {large_iteration_index}!");
    for i in 1..10 {
        let referer_thing_api: Api<RefererThing> = Api::default_namespaced(client.clone());
        let updated_referer = json!({
            "spec": {
                "value": i
            }
        });

        referer_thing_api.patch("my-referer", &PatchParams::default(), &Patch::Merge(updated_referer)).await?;
        info!("updated referer resource {i} in {large_iteration_index}");

        let _ = stream.next().await.unwrap();

        info!("Handled updated reconciliation");
    }

    info!("Finished large iteration {large_iteration_index}");
    Ok(())
}

async fn reconcile_main_thing(resource: Arc<MainThing>, _context: Arc<()>) -> Result<Action, Error> {
    info!("Reconciling {}", resource.name_any());
    sleep(Duration::from_millis(1)).await;

    Ok(Action::await_change())
}

fn error_policy(_resource: Arc<MainThing>, _error: &Error, _context: Arc<()>) -> Action {
    Action::requeue(Duration::from_secs(15))
}


/// All errors possible to occur during reconciliation
#[derive(thiserror::Error, Debug)]
#[error(transparent)]
pub struct Error(#[from] anyhow::Error);


async fn ensure_crds() -> Result<()> {
    let client = Client::try_default().await?;

    // Manage CRDs first
    let crds: Api<CustomResourceDefinition> = Api::all(client.clone());

    // Create the CRD so we can create MainThings in kube
    let main_thing_crd = MainThing::crd();
    let patch_params = PatchParams::apply("example-crd-watcher").force();
    crds.patch(&main_thing_crd.name_any(), &patch_params, &Patch::Apply(main_thing_crd)).await?;

    // Create the CRD so we can create RefererThings in kube
    let referer_thing_crd = RefererThing::crd();
    crds.patch(&referer_thing_crd.name_any(), &patch_params, &Patch::Apply(referer_thing_crd)).await?;

    // Wait for the api to catch up
    sleep(Duration::from_secs(1)).await;

    let main_thing_api: Api<MainThing> = Api::default_namespaced(client.clone());
    let referer_thing_api: Api<RefererThing> = Api::default_namespaced(client.clone());

    let main_thing_instance = MainThing {
        metadata: ObjectMeta {
            name: Some("my-main-thing".into()),
            ..Default::default()
        },
        spec: MainThingSpec {},
    };

    let main_thing_instance = main_thing_api.patch(&main_thing_instance.name_any(), &patch_params, &Patch::Apply(main_thing_instance)).await?;

    let referer_instance = RefererThing {
        metadata: ObjectMeta {
            name: Some("my-referer".into()),
            ..Default::default()
        },
        spec: RefererThingSpec {
            main_thing_name: main_thing_instance.name_any(),
            main_thing_namespace: None,
            value: 0,
        },
    };

    let _referer_instance = referer_thing_api.patch(&referer_instance.name_any(), &patch_params, &Patch::Apply(referer_instance)).await?;

    info!("All test resources created");

    Ok(())
}