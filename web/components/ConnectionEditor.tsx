"use client";

import { useRef, useState } from "react";
import { useActionState } from "react";
import { stringify } from "yaml";
import { applyRawResourceAction } from "@/app/actions";
import { initialFormActionState } from "@/lib/form-state";
import { buildConnectionSpec, parseYamlDocument } from "@/lib/resourceForm";
import type { ConnectionResource } from "@/lib/controlplane";
import { Button } from "@/components/ui/button";
import ConnectionForm from "@/components/ConnectionForm";
import YamlResourceEditor from "@/components/YamlResourceEditor";

export default function ConnectionEditor({
  mode,
  initial,
}: {
  mode: "create" | "edit";
  initial?: ConnectionResource;
}) {
  const [editorMode, setEditorMode] = useState<"form" | "yaml">("form");
  const [formKey, setFormKey] = useState(0);
  const [currentInitial, setCurrentInitial] = useState(initial);
  const [yamlText, setYamlText] = useState("");
  const [yamlError, setYamlError] = useState<string>();
  const formRef = useRef<HTMLFormElement>(null);

  const [yamlState, yamlAction, yamlPending] = useActionState(
    applyRawResourceAction.bind(null, "Connection", mode === "edit" ? initial?.name : undefined),
    initialFormActionState,
  );

  function switchToYaml() {
    const fd = new FormData(formRef.current!);
    const name = String(fd.get("name") ?? currentInitial?.name ?? "");
    const spec = buildConnectionSpec(fd);
    setYamlText(
      stringify({ apiVersion: "dal.io/v1alpha1", kind: "Connection", metadata: { name }, spec }),
    );
    setYamlError(undefined);
    setEditorMode("yaml");
  }

  function switchToForm() {
    const doc = parseYamlDocument(yamlText);
    if (!doc.ok) {
      setYamlError(doc.error);
      return;
    }
    if (doc.kind !== "Connection") {
      setYamlError('kind must be "Connection"');
      return;
    }
    setCurrentInitial({
      kind: "Connection",
      name: doc.name,
      spec: doc.spec as ConnectionResource["spec"],
      raw: yamlText,
    });
    setFormKey((k) => k + 1);
    setYamlError(undefined);
    setEditorMode("form");
  }

  return (
    <div className="grid gap-4">
      <div className="inline-flex w-fit overflow-hidden rounded-lg border border-border">
        <Button
          type="button"
          variant={editorMode === "form" ? "secondary" : "ghost"}
          size="sm"
          className="rounded-none"
          onClick={switchToForm}
        >
          Form
        </Button>
        <Button
          type="button"
          variant={editorMode === "yaml" ? "secondary" : "ghost"}
          size="sm"
          className="rounded-none"
          onClick={switchToYaml}
        >
          YAML
        </Button>
      </div>

      {editorMode === "form" ? (
        <ConnectionForm key={formKey} ref={formRef} mode={mode} initial={currentInitial} />
      ) : (
        <form action={yamlAction} className="grid gap-4">
          {(yamlState.formError || yamlError) && (
            <p role="alert" className="rounded-lg bg-destructive/10 p-3 text-sm text-destructive">
              {yamlState.formError ?? yamlError}
            </p>
          )}
          <input type="hidden" name="yaml" value={yamlText} />
          <YamlResourceEditor value={yamlText} onChange={setYamlText} />
          <div>
            <Button type="submit" disabled={yamlPending}>
              {mode === "create" ? "Create connection" : "Save changes"}
            </Button>
          </div>
        </form>
      )}
    </div>
  );
}
