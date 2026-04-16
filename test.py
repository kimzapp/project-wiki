from optimum.exporters.onnx import main_export

main_export(
    model_name_or_path="vinai/PhoGPT-4B",
    output="phogpt_onnx",
    task="text-generation",
    opset=14,
    use_cache=True
)