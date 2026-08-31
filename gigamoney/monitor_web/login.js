"use strict";

const form = document.getElementById("login-form");
const username = document.getElementById("username");
const password = document.getElementById("password");
const button = document.getElementById("login-button");
const error = document.getElementById("login-error");

form.addEventListener("submit", async (event) => {
  event.preventDefault();
  error.textContent = "";
  button.disabled = true;
  button.textContent = "Signing in…";
  try {
    const response = await fetch("/api/login", {
      method: "POST", cache: "no-store",
      headers: { Accept: "application/json", "Content-Type": "application/json" },
      body: JSON.stringify({ username: username.value, password: password.value }),
    });
    const payload = await response.json().catch(() => ({}));
    if (!response.ok || !payload.ok) throw new Error(payload.message || "Sign-in failed.");
    password.value = "";
    window.location.replace("/");
  } catch (reason) {
    error.textContent = reason.message || "Could not reach the command center.";
    password.select();
  } finally {
    button.disabled = false;
    button.textContent = "Sign in";
  }
});
