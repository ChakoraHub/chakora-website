from kivy.app import App
from kivy.uix.screenmanager import Screen
from kivy.core.window import Window
from kivy.lang import Builder




class EnquiryScreen(Screen):
    def submit_form(self):
        """Collect form data and print/save it"""
        name = self.ids.name.text.strip()
        email = self.ids.email.text.strip()
        phone = self.ids.phone.text.strip()
        enquiry = self.ids.enquiry.text.strip()

        if name and email and phone and enquiry:
            print("✅ New Enquiry Submitted:")
            print(f"Name: {name}")
            print(f"Email: {email}")
            print(f"Phone: {phone}")
            print(f"Enquiry: {enquiry}")
            print("-" * 30)

            # Clear fields after submission
            self.ids.name.text = ""
            self.ids.email.text = ""
            self.ids.phone.text = ""
            self.ids.enquiry.text = ""
        else:
            print("⚠ Please fill all fields before submitting.")

    def go_back(self):
        """Simulate going back to resources"""
        print("⬅ Back to Resources clicked!")


class EnquiryApp(App):
    def build(self):
        return Builder.load_file("enquiry.kv")


if __name__ == "__main__":
    EnquiryApp().run()