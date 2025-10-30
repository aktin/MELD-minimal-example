import ModelEnvironment.model
from ModelEnvironment.model import build_artifact

m = ModelEnvironment.model.load_model("model")

build_artifact(m, "model.onnx")