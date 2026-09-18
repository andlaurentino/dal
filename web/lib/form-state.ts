export interface FormActionState {
  ok: boolean;
  formError?: string;
  fieldErrors?: Record<string, string>;
}

export const initialFormActionState: FormActionState = { ok: true };
